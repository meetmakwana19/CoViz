# CoViz

These are python scripts programmed to fetch data from Indian government's CoWIN Public API regarding vaccination slots.
The fetch data is been stored in databases files.
SQLite3 is used for database connectivity.

There are server sider issues of cached response while fetching the data from the API. So when the API responses it sends a cached data of about past 5-10 minutes which is even stated in their documentation listed at https://apisetu.gov.in/public/api/cowin/cowin-public-v2

Would love to hear your views on the solution of caching.
