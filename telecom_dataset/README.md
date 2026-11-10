# Telecom Customer Churn Dataset

## Source Attribution
- **Source:** Kaggle — [Customer Churn Dataset (barun2104)](https://www.kaggle.com/datasets/barun2104/telecom-churn)
- **Origin:** MLC++ Machine Learning Library benchmark modelled after US telecommunications carriers.
- **Records:** 3,333 unique customers.
- **Base Churn Rate:** 14.49% (483 churned, 2,850 active).

---

## Schema & Attribute Dictionary

| Attribute | Data Type | Description | Unit / Values |
|---|---|---|---|
| `AccountWeeks` | Integer | Length of time the customer has held the active account | Weeks |
| `ContractRenewal` | Binary (0 / 1) | Whether the customer recently renewed their service contract | `1` = Renewed, `0` = Not Renewed |
| `DataPlan` | Binary (0 / 1) | Whether the customer subscribes to a wireless data plan | `1` = Active Data Plan, `0` = Voice Only |
| `DataUsage` | Float | Average monthly mobile data consumption | Gigabytes (GB) |
| `CustServCalls` | Integer | Total customer service calls made during the billing period | Count |
| `DayMins` | Float | Total daytime call minutes per month | Minutes |
| `DayCalls` | Integer | Total count of daytime telephone calls placed | Count |
| `MonthlyCharge` | Float | Base monthly bill charge before taxes or overages | USD ($) |
| `OverageFee` | Float | Additional charges incurred for usage exceeding plan allowances | USD ($) |
| `RoamMins` | Float | Total roaming call minutes placed outside home coverage area | Minutes |
| `Churn` | Binary (0 / 1) | Target indicator representing whether the customer terminated service | `1` = Churned, `0` = Active |

---

## Event Simulation Design

In real telecom environments, customer transactions (calls, data usage, top-ups, complaints) happen on a continuous basis. To create a realistic streaming architecture without data leakage:

1. **Daily Call Events:** Sampled using a Poisson distribution parameterized by each customer's `DayCalls / 30` and `DayMins`.
2. **Data Session Events:** Generated from `DataUsage / 30`.
3. **Recharge Events:** Triggered periodically based on `MonthlyCharge` and `OverageFee`.
4. **Complaint Events:** Probability sampled each day from `CustServCalls / 30`.
5. **No Label Leakage:** The target `Churn` column is **never** used during simulation. Spark MLlib discovers patterns purely through the emergent behavioral signals in the streaming logs.
