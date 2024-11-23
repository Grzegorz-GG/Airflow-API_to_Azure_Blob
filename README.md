<p>Airflow pipeline which fetches the data from API and loads it to the Azure Blob Container. It consists of the following DAGs:</p>
<br>
get_weather_data >> process_data >> upload_to_blob
<br>
<p>The data for a specific latitude and longitude is fetched from weather API. Nextly, it is transformed locally using Python. Finally, uploaded to Azure Blob storage as json.

  
</p>

