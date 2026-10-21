# Financial Forecasting Frontier: Distributed ML

Official project: https://grow.almabetter.com/masters-data-science/learn/masters-in-data-science/ml-engineering-v1-uj-15/181/capstone-projects/96

Dataset: https://drive.google.com/file/d/1B9H8V9BS662llULSjp_DM_Lh8TQ1dgwZ/view

Downloaded original CSV: 4,521 records, 17 columns, 521 yes and 4,000 no labels; no duplicate rows.
SHA256: c7764aef15407fa186de79c2628f42ab28845b10a9390493f60e984ca4eff4ca

Complete all five parts: Hadoop/Hive; Spark EDA; Spark ML; Spark Streaming; data parallelism.

## Grading

Distributed concepts 10%; data management 15%; EDA and preprocessing 15%; modeling 15%; streaming 10%; parallelism 10%; innovation 5%; documentation and presentation 20%.

## Official final checklist

- Separate Google Docs or Colab files with code and output for all five parts.
- All local source code, including Hadoop, Hive, Spark and streaming scripts.
- Streaming sample files.
- Video covering questions in all five parts, at least 15 minutes.
- Brief reflective summary in a Google Doc.
- Put documents, notebooks, scripts and data in Google Drive. Submit folder under Project link and recording under Video link.
- Individual submission; make notebooks viewable to reviewers.

## Honest interpretation of unavailable fields

No year or complete date exists: year-over-year contact trends cannot be calculated. Month is not a substitute for year.
No loan amount exists: loan is yes/no. Use loan prevalence/count by age group as a labelled alternative, never as an amount.
Default is a current credit-default indicator, not a labelled future default probability.
Duration describes a marketing call, not a payment transaction. Streaming replays simulated marketing records with synthetic event timestamps; it is not a fraud detection model.
Duration is excluded from the pre-call subscription model to prevent post-contact leakage.
This is a small sample, so parallel execution demonstrates the method without claiming that distribution is faster.
