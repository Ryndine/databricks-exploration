# Databricks Exploration (Untitled Project)
Since I cannot connect Databricks to GitHub using the community version, this repository exists as a dump for my databricks notebooks.  

## Objective: 
Utilizing the Databricks platform to create a more efficient work envrionment for my project teams.  
Create models and visualization tools for analyzing video game user trends utilizing the databricks platform.

## Tools | Database
<table border="0" >
 <tr>
    <td width="200px"><li>Python</li></td>
    <td width="200px"><li>Scala</li></td>
    <td width="200px"><li>Spark</li></td>
 </tr>
 <tr>
    <td width="200px"><li>SQL</li></td>
    <td width="200px"><li>Hive</li></td>
    <td width="200px"><li>Tableau</li></td>
 </tr>
 <tr>
    <td width="200px"><li>APIs</li></td>
    <td width="200px"><li>Databricks</li></td>
    <td width="200px"><li>S3</li></td>
 </tr>
  <tr>
    <td width="200px"><li>Splinter</li></td>
    <td width="200px"><li>Beautiful Soup</li></td>
    <td width="200px"><li>Flask/Dash</li></td>
 </tr>
</table>

## Outline
Observe the impact Steam featured page has on game viewership on Twitch.  
Expand project beyond initial scope to observe possible trends with updates/news.  
Explore marketing optimizations for game companies.

**API / Scraping**
- Twitter API to collect game viewership
- Scrape featured / updated games from steam store front and run API calls for those games
- Steam Store API calls to collect user data based on scraped games.
- (Optional) Expand API / Scraping to additonal platforms for more data such as Youtube.
- Setup scripts to run daily.

**Database / S3**
- Store data into AWS Databricks.
- Setup scripts to pull only new data, and clean for analysis tools.
- Utilize S3 to share files between team (GitHub connectivity not available for Databricks CE).

**Models**
- Explore best models for predicting trends.

**Visualization**  
- Create a dashboard app to allow for user friendly interactive visuals and filtering for exploration into data.
