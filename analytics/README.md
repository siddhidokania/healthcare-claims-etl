# Analytics: Excel, Power BI and Tableau

Reporting on top of the pipeline's gold layer.

```bash
# 1. export gold tables and summary marts (after the pipeline has run)
docker compose run --rm runner python -m pipeline.export_bi
#    -> data/exports/star/   fact_claims.csv + dim_patient/provider/diagnosis/date.csv  (Power BI, Tableau)
#    -> data/exports/marts/  5 small summary tables                                       (Excel)

# 2. rebuild the Excel workbook from the marts
docker build -t claims-analytics -f docker/analytics/Dockerfile docker/analytics
docker run --rm -v "$PWD:/app" -w /app claims-analytics python analytics/build_excel_workbook.py
```

## Excel workbook: [claims_analysis.xlsx](claims_analysis.xlsx)

| Sheet | What it shows |
|---|---|
| Summary | KPIs, payment and claims by year and claim type with year-over-year change, top 10 ICD-9 chapters by payment, two charts |
| Monthly Trend | 36-month inpatient and outpatient payment trend with a line chart |
| Chronic Conditions | Prevalence and average annual reimbursement with vs without each of 11 conditions; **pick the year in the yellow cell** |
| Inpatient Stays | Stays and average payment by length-of-stay band |
| Data_* | The exported marts as Excel tables (filterable) |
| Notes | Sources and definitions |

All 351 summary cells are `SUMIFS`/`IF` formulas over the data sheets; nothing is pasted as a value.

Findings (2008-2010):
- **$860.3M** of Medicare payments over **845,706** claims; inpatient claims are 7.9% of claims but **74%** of payment.
- **Circulatory system** diagnoses account for the largest share of payment (**21.8%**).
- Patients with **chronic kidney disease** cost **4.5x** more per year than patients without it (2010), the highest of the 11 conditions.
- Average inpatient payment rises from **$6,919** for 0-1 day stays to **$18,968** for stays of 15+ days.

## Power BI report (build it yourself in Power BI Desktop, free)

1. Install **Power BI Desktop** from the Microsoft Store (free).
2. **Get data → Text/CSV**, load all five files from `data/exports/star/`.
3. **Power Query** (Transform data): set `payment_amt`, `deductible_amt`, `coinsurance_amt` to *Fixed decimal*; `birth_date`, `effective_from`, `effective_to`, `full_date` to *Date*; `has_*`, `is_current`, `is_adjustment` to *True/False*; keep `state_code` as *Text* so leading zeros stay.
4. **Model view**, create the relationships (all one-to-many, single direction):
   - `dim_patient[patient_sk]` → `fact_claims[patient_sk]`
   - `dim_provider[provider_sk]` → `fact_claims[provider_sk]`
   - `dim_diagnosis[diagnosis_sk]` → `fact_claims[primary_diagnosis_sk]`
   - `dim_date[date_key]` → `fact_claims[from_date_key]`
   - Mark `dim_date` as the date table (*Table tools → Mark as date table → full_date*).
5. Add the measures from [powerbi/measures.dax](powerbi/measures.dax).
6. Build three pages:
   - **Overview:** cards (Total Payment, Claims, Patients, Avg Payment per Claim), line chart of Total Payment by month split by claim_type, bar chart of Total Payment by `icd9_chapter`, slicers for year and claim type.
   - **Patients:** stacked bar of Patients by `chronic_condition_count`, matrix of Payment per Patient by `sex` and `race`, map or bar of Total Payment by `state_code`.
   - **Inpatient:** Avg Length of Stay by year, scatter of Avg Payment per Claim vs Avg Length of Stay by `icd9_chapter`.
7. Save as `analytics/powerbi/claims_report.pbix`, take a screenshot of each page into `analytics/powerbi/`, and commit them.

## Tableau dashboard (Tableau Public, free)

1. Install **Tableau Public** (free) and sign up for a Tableau Public profile.
2. **Connect → Text file**, open `fact_claims.csv`, then drag `dim_diagnosis.csv`, `dim_patient.csv` and `dim_date.csv` onto the canvas and relate them with the same keys as above.
3. Build worksheets: payment by month (line), payment by ICD-9 chapter (bar), claims by patient state (map or bar), average length of stay by year.
4. Combine them in a dashboard with a claim-type filter, **publish to Tableau Public**, and put the link in this README and on your resume.
