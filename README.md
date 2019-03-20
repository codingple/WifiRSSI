# WifiRSSI
#### WiFi RSSI analysis system for adjusting access point position  

### Motivation  
When I was in college, WiFi provided by the university was not working well. 
The Access Points are deployed on aisle, and I thought the problem is 
that weak signal reaches to my phone. 
So, I decided to check WiFi RSSI to tune the APs' position, and make the visualization of the degree.  
However, RSSI value is not stable and there is no standard for estimating good or bad strength.  
For this reason, I just made grade for myself.  
  
### Description  
_Note : I was in charge of data processing using Hadoop. Sorry that I cannot open other source code_  
  
There are 3 modules: Data collecting application, data processing, web service.  
Using data collecting app, the RSSI data is collected with locational information. 
When you collect data outdoor, the GPS data will be contained. 
On the other hand, you should insert the indoor location like class room number. 
The collected data is processed by Hadoop. In this process, 
1st MapReduce Job counts the number of RSSI data class and total data. 
After the grade is estimated, 2nd MapReduce Job insert the grade to each row. 
Finally, this information is visualized through the web service.  
  
### Demonstration Video  
You can see the demonstration video for each module in here:  
* **Data Collecting Application**  
![DCA](https://www.youtube.com/watch?v=FVGyEcZ3yXQ)  
&nbsp;  
* **Data Processing**  
![DP](https://www.youtube.com/watch?v=LmmYxxK5_dA&t=37s)  
&nbsp;  
* **Web Service**  
![WS](https://www.youtube.com/watch?v=k3jNL8_YfCM)
