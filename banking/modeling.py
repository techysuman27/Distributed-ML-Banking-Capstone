from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer, StringIndexer, OneHotEncoder, Imputer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from banking.common import SCHEMA,CAT,NUM

def transform_statement():
    return '''SELECT *, SIGN(balance)*LOG(1+ABS(balance)) AS balance_log,
    LOG(1+GREATEST(campaign,0)) AS campaign_log,
    LOG(1+GREATEST(previous,0)) AS previous_log FROM __THIS__'''

def stable_split(df):
    # Source rows have no IDs. A full-row stable hash keeps identical records in the same split.
    key=F.to_json(F.struct(*[F.col(c) for c in SCHEMA.fieldNames()]))
    df=df.withColumn('_split',F.pmod(F.xxhash64(key),F.lit(5)))
    return df.filter(F.col('_split')!=0).drop('_split'),df.filter(F.col('_split')==0).drop('_split')

def make_pipeline(model):
    numeric=[{'balance':'balance_log','campaign':'campaign_log','previous':'previous_log'}.get(c,c) for c in NUM]
    indexed=[c+'_idx' for c in CAT]
    encoded=[c+'_vec' for c in CAT]
    imputed=[c+'_imputed' for c in numeric]
    return Pipeline(stages=[
        SQLTransformer(statement=transform_statement()),
        Imputer(inputCols=numeric,outputCols=imputed,strategy='median'),
        StringIndexer(inputCols=CAT,outputCols=indexed,handleInvalid='keep'),
        OneHotEncoder(inputCols=indexed,outputCols=encoded,handleInvalid='keep'),
        VectorAssembler(inputCols=imputed+encoded,outputCol='raw_features'),
        StandardScaler(inputCol='raw_features',outputCol='features',withMean=False,withStd=True),
        model])

def evaluate(pred):
    counts={(int(r.label),int(r.prediction)):r['count'] for r in pred.groupBy('label','prediction').count().collect()}
    tn,fp,fn,tp=[counts.get(k,0) for k in [(0,0),(0,1),(1,0),(1,1)]]
    precision=tp/(tp+fp) if tp+fp else 0
    recall=tp/(tp+fn) if tp+fn else 0
    result={'accuracy':(tp+tn)/(tp+tn+fp+fn),'precision':precision,'recall':recall,'f1':2*precision*recall/(precision+recall) if precision+recall else 0,'confusion':{'tn':tn,'fp':fp,'fn':fn,'tp':tp}}
    for name,metric in [('roc_auc','areaUnderROC'),('pr_auc','areaUnderPR')]:
        result[name]=BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='probability',metricName=metric).evaluate(pred)
    return result

def feature_importance(pipeline,df):
    metadata=pipeline.transform(df.limit(1)).schema['raw_features'].metadata['ml_attr']['attrs']
    names={a['idx']:a['name'] for group in metadata.values() for a in group}
    model=pipeline.stages[-1]
    values=model.coefficients if hasattr(model,'coefficients') else model.featureImportances
    return sorted([{'feature':names.get(i,str(i)),'value':float(v)} for i,v in enumerate(values)],key=lambda x:abs(x['value']),reverse=True)
