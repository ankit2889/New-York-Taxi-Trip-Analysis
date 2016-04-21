# New-York-Taxi-Trip-Analysis

1. Presentation link for Prezi: https://prezi.com/u6mhos07uy3n/finalprojectppt/?utm_campaign=share&utm_medium=copy
2. Merged 2 datasets to get trip with fare details and pickup and drop off point.
2. Created case classes trip and point.
3. Used RichGeometry class to check if point contains within borough.
4. Used GeoJson class to parse the JSON data.
5. Pre-processed data now has borough id (pickup and dropoff) along with other trip data.Used dataframes to calculate overall brownie    point with formula: zi = xi*yi/300 (i = 0 to 2) 
6. Created model after applying linear regression and achieved acceptance criteria of R.M.S = 36
7. Identified top 3 drop off locations outside Manhattan by applying K means.
8. Data visualization was done using D3.js and map visualization using CartoDB.


 
