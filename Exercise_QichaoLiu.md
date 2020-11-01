## Business Intelligence Exercise

Author: Qichao Liu

### 1. Write a SQL script to create and populate data in a table to report performance of our conversion funnel by important dimensions

Overall I will develop a ETL process to extract, transform, clean and load data into a table which can be updated daily.

* Assumption
    * I assume the final table named CoversationRate
    * Assume it has a lot of new data flowing into the table, so I will append everyday new data to table with reporting date.
* Data cleaning
    * For the records that doesn't have previous step, for example, if there is only Accessed Store Page but no HP Visits
    before it, it's a invalid and bad data which will not be included when calculate the first step conversation rate.
    * For the duplicated rows since there is no any other information can differentiate them, I would assume it's a duplicated
    records and remove them.
* Dimension
    * Day of week: help to check whether conversation rate varies based on day of week
    * Region: help to check whether conversation rate varies based on different region
    * platform: help to check whether conversation rate varies based on different region
    * initial_referring_domain: help to check whether conversation rate varies based on different referring domain. 
* Data field to count: we can use different unit to calculate the conversion rate. I will use the first one of following unit. 
    * event: Each event the user generate each time will be a unit.
    * userID: Each user will be unit, count the number of users when calculate.
    * sessionID: classify user'a activity into different session, assume if the step is checkout_success or the minutes difference 
    between two step is more than 30 minutes then the current session is over.
    * Trade off by using different field to count:
        * Using event will accurately capture all the events in each step and the events which leading to next step. It can tell us how
        many times user will explore the home page before they move to next step. But it will 
        not reveal any user information, user conversation rate, and session conversation rate. User might mistakenly refresh page many times
        or the count can be depend on the different device setting. It might not reflect actual conversation rate. 
        * Using UserId to count can exactly capture the user conversation rate which can help us to expand the market and doing 
        further user activity analysis. But it cannot reveal the conversation rate for event. It might be the case that user will try a 
        lot of times before they actually place on order or there is some user experience issue at one step.
        * Using session can exactly tell us the successful conversation rate for each step when a user tries to place an order. It can help us to check
        which step most of time people is struggling and give up. But it cannot tell us more about event, the user might switch between
        two steps multiple time then give up, it also might be possible that user just open home_page once then give up.   

    
#### Before start I will create and design the final table structure

````
CREATE TABLE CoversationRate
(
    ReportingDate Date,
    Dimensionvalue varchar(50),
    Dimension varchar(50),
    NumberOfHP int,
    NumberOfHPtoSop int,
    NumberOfSop int,
    NumberOfSopTOCP int,
    NumberOfCP int,
    NumberOfCpTOCS int,
    HomePageToStoreOrderConversionRate Decimal(10,2),
    StoreOrderToCheckoutPageConversionRate Decimal(10,2),
    HomePageToStoreOrderConversionRate Decimal(10,2)
    
)
```` 

#### A. In order to calculate the conversation rate, I follow the following steps to pull the data from table and do the calculation

* For each user, find the next event_type based on the time ascending sort within the same device
* Calculate the number of the visits for each step 
* Calcualte the number of the visits which has the next step following it.
* Divide the total number of visits for each step by number of the visits that have leading to the next step
* Insert the daily output to the final table.

```

DROP TABLE IF EXISTS #TodayData
SELECT
    GETDATE() AS ReportingDate,
    CASE WHEN a.initial_referring_domain=' ' THEN 'No initial_referring_domain' ELSE a.initial_referring_domain END AS DimensionValue,
    'initial_referring_domain' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
INTO #TodayData
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY  user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY 
       CASE WHEN a.initial_referring_domain=' ' THEN 'No initial_referring_domain' ELSE a.initial_referring_domain END
UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    CASE WHEN a.region=' ' THEN 'NO Region' ELSE a.region END AS DimensionValue,
    'Region' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) over (partition by user_id,device_id order by event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY a.region


UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    a.platform AS DimensionValue,
    'platform' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY a.platform


UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    DATENAME(dw,a.event_time) AS DimensionValue,
    'DayofWeek' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY DATENAME(dw,a.event_time)


INSERT INTO CoversationRate
SELECT
    GETDATE(),
    DimensionValue,
    Dimension,
    NumberofHP,
    NumberofHpToSOP,
    NumberofSop,
    NumberofSopToCp,
    NumberofCP,
    NumberofCPtoCS,
    HomePageToStoreOrderConversionRate,
    StoreOrderToCheckoutPageConversionRate,
    CheckoutPageToCheckoutSuccessConversionRate
FROM #TodayData
```


### 2. Make a recommendation of how we should include search as part of the funnel and modify your SQL script and include it in your final table.

Search is the functionality of the DoorDash to help the user find their interested restaurant/food. The best way to decide 
how to include use of search is to figure out how the use of search impact conversion funnel. Based on the data analysis, sometime user will 
go from Homge_page to Search_event then to the Store_page rather than go directly from home_page to store_page. 

#### A. Checking the search conversion rate to different steps by using following query 

````
SELECT
sum(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/(1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)) AS SearchToStoreOrdering,
sum(CASE WHEN event_type='search_event' and nextevent='home_page' THEN 1 ELSE 0 END)/(1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)) AS SearchtoHomePage,
sum(CASE WHEN event_type='search_event' and nextevent='checkout_page' THEN 1 ELSE 0 END)/(1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)) AS searchToCheckoutPage,
sum(CASE WHEN event_type='search_event' and nextevent='checkout_success' THEN 1 ELSE 0 END)/(1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)) AS SeachToCheckoutSuccess
FROM (
         SELECT DISTINCT 
         		ue.event_time,
                user_id,
                event_type,
                LEAD(event_type) over (partition by user_id,device_id order by event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
````

* It turns out if the user in the Search event, it has around 52% possibility going to store_page event, see following conversion rate to each step.

| SearchToStoreordering        | SearchToHomePage           | SeachToCheckoutPage  |  SeachToCheckoutSuccess    |
| ------------- |:-------------:| -----:| ---: |
| 0.524       | 0.160 | 0.008 |   0   |

* So we should:
    * Add conversion rate from home_page to search and from search to store_page. 
    * Create a new HomePagetoStoreOrder conversation rate which take into account the search impact.
    * These three new conversations can help us to clearly understand that if we could improve the search functionality, 
    and guide more user go from home_page to search_event before
    go to store_page, and it will finally increase the overall conversation rate from home_page to store_page  

* In order to insert new metrics into the table have to drop and re-create the table structure. 

````
DROP TABLE CoversationRate

CREATE TABLE CoversationRate
(
    ReportingDate Date,
    Dimensionvalue varchar(50),
    Dimension varchar(50),
    NumberOfHP int,
    NumberOfHPtoSop int,
    NumberOfSop int,
    NumberOfSopTOCP int,
    NumberOfCP int,
    NumberOfCpTOCS int,
    NumberofSearchToSop int,
    NumberofSeach int,
    HomePageToStoreOrderConversionRate Decimal(10,2),
    HomePageToSeach Decimal(10,2)
    SearchToStoreOrdering Decimal(10,2),
    HomePageToStoreOrderConversionRateWithSearch Decimal(10,2),
    StoreOrderToCheckoutPageConversionRate Decimal(10,2),
    HomePageToStoreOrderConversionRate Decimal(10,2)
    
)
````


* The modified query is as follows:

````


DROP TABLE IF EXISTS #TodayData
SELECT
    GETDATE() AS ReportingDate,
    CASE WHEN a.initial_referring_domain=' ' THEN 'No initial_referring_domain' ELSE a.initial_referring_domain END AS DimensionValue,
    'initial_referring_domain' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    SUM(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSearchToSop,
    SUM(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END) AS NumberofSeach,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='search_event' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToSearch,
    ROUND(sum(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS SearchToStoreOrdering,
    ROUND(SUM(CASE WHEN (event_type='home_page' and nextevent='store_ordering_page') OR (event_type='search_event' and nextevent='store_ordering_page')  THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' OR event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRateWithSearch,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
INTO #TodayData
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY  user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY
       CASE WHEN a.initial_referring_domain=' ' THEN 'No initial_referring_domain' ELSE a.initial_referring_domain END
UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    CASE WHEN a.region=' ' THEN 'NO Region' ELSE a.region END AS DimensionValue,
    'Region' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    SUM(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSearchToSop,
    SUM(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END) AS NumberofSeach,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='search_event' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToSearch,
    ROUND(sum(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS SearchToStoreOrdering,
    ROUND(SUM(CASE WHEN (event_type='home_page' and nextevent='store_ordering_page') OR (event_type='search_event' and nextevent='store_ordering_page')  THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' OR event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRateWithSearch,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) over (partition by user_id,device_id order by event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY a.region


UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    a.platform AS DimensionValue,
    'platform' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    SUM(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSearchToSop,
    SUM(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END) AS NumberofSeach,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='search_event' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToSearch,
    ROUND(sum(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS SearchToStoreOrdering,
    ROUND(SUM(CASE WHEN (event_type='home_page' and nextevent='store_ordering_page') OR (event_type='search_event' and nextevent='store_ordering_page')  THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' OR event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRateWithSearch,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY a.platform


UNION ALL

SELECT
    GETDATE() AS ReportingDate,
    DATENAME(dw,a.event_time) AS DimensionValue,
    'DayofWeek' AS Dimension,
    SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END) AS NumberofHP,
    SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofHpToSOP,
    SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSop,
    SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END) AS NumberofSopToCp,
    SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END) AS NumberofCP,
    SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END) AS NumberofCPtoCS,
    SUM(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END) AS NumberofSearchToSop,
    SUM(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END) AS NumberofSeach,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRate,
    ROUND(SUM(CASE WHEN event_type='home_page' and nextevent='search_event' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' THEN 1 ELSE 0 END)),0),2) AS HomePageToSearch,
    ROUND(sum(CASE WHEN event_type='search_event' and nextevent='store_ordering_page' THEN 1 ELSE 0 END)/NULLIF((1.0*sum(CASE WHEN event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS SearchToStoreOrdering,
    ROUND(SUM(CASE WHEN (event_type='home_page' and nextevent='store_ordering_page') OR (event_type='search_event' and nextevent='store_ordering_page')  THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='home_page' OR event_type='search_event' THEN 1 ELSE 0 END)),0),2) AS HomePageToStoreOrderConversionRateWithSearch,
    ROUND(SUM(CASE WHEN event_type='store_ordering_page' and nextevent='checkout_page' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='store_ordering_page' THEN 1 ELSE 0 END)),0),2) AS StoreOrderToCheckoutPageConversionRate,
    ROUND(SUM(CASE WHEN event_type='checkout_page' and nextevent='checkout_success' THEN 1 ELSE 0 END)/NULLIF((1.0*SUM(CASE WHEN event_type='checkout_page' THEN 1 ELSE 0 END)),0),2) AS CheckoutPageToCheckoutSuccessConversionRate
FROM (
         SELECT DISTINCT
                ue.event_time,
                user_id,
                event_type,
                platform,
                region,
                initial_referring_domain,
                LEAD(event_type) OVER (PARTITION BY user_id,device_id ORDER BY event_time) AS nextevent
         FROM csn_junk.dbo.user_events ue
     ) AS a
GROUP BY DATENAME(dw,a.event_time)


INSERT INTO CoversationRate
SELECT
    GETDATE(),
    DimensionValue,
    Dimension,
    NumberofHP,
    NumberofHpToSOP,
    NumberofSop,
    NumberofSopToCp,
    NumberofCP,
    NumberofCPtoCS,
    NumberofSearchToSop,
    NumberofSeach,
    HomePageToStoreOrderConversionRate,
    HomePageToSearch,
    SearchToStoreOrdering,
    HomePageToStoreOrderConversionRateWithSearch,
    StoreOrderToCheckoutPageConversionRate,
    CheckoutPageToCheckoutSuccessConversionRate
FROM #TodayData

````