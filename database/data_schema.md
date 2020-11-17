# User Queries

**Parameters**

- County e.g Galway, Dublin, Carlow, all

- Listing type: buy or rent
- Property type: apartment or house
- Property class: {1, 2, 3, other}apt,   {2, 3, other}hse, 
- Time frame: this month, previous month, x-months



**Example**

Tell me about <u>rental</u> properties in <u>Dublin</u> 

> Return avg, median, and volumes stats for this month



# Read path

1. Check database for the last update
2. Tell the puller to fetch new data from the last update onwards
3. Store fresh data in the recent_data table
4. Package up relevant data  to the query and sent the message



# Databases

### Database 1: Daft.ie 

**Table 1: historic_data**

**Columns**

Date, County, MedApt1, AvgApt1,VolApt1,MedApt2,AvgApt2,VolApt2,MedApt3, AvgApt3,VolApt3,MedHse2, AvgHse2,VolHse2,MedHse3, AvgHse3, VolHse3,VolApt,VolHse

**Table 2: recent_data**

**Columns**

Date, County, Listing type, Property type, Property class, Price



### Database 2: Myhome.ie 

Ditto