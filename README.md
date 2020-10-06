# Starbucks_CapstoneProject


# Starbucks_CapstoneProject

https://medium.com/@stefan.ludmann/the-quick-and-dirty-way-to-evaluate-offers-for-clusters-12f43f44dc06?source=friends_link&sk=045bed1e2c862dad072605f18da809de



READ me:

1.	Project Motivation
2.	Installation and Files
3.  	Results
4.	Licensing, Authors and Acknowledgements


1.   Project Motivation:
	
The Problem I am facing is that offers and can on one hand increase profit, but if they were sent out to the wrong people, they can reduce the margin.
The Questions can be formulated like this: When is an offer successful?

I do implement a Feature called Influence-Score, that is measuring the likelihood that the offer attracted the customer to step by on an extra visit. That is in my eyes the goal of any offer. 

In the end I will show plots, how many of the sent out offers where used in a regular visit (unsuccessful completion) or as part of an extra visit (successful offer). In-between there is a third category – that cannot be differentiated clearly. 



2.   Installation and Files
--- How to Part ---

I have split the Process in an ETL and one ML part. The ML Part is where the magic happens, you find it above as Jupyter Notebook: 'ML StarbucksProject'.
The ML part does include the Feature creation and the Clustering. Both are time consuming, but also both can be adapted and adjusted, that is why they are part of this second Part.


2. 1   ETL-Part:

Data is provided by Starbucks
3 files:
-	Portfolio
-	Profiles
-	Transcript

Transcript is the first file to proceed to get all the values (consist on offer-id and amount) extracted. I then use the Portfolio to add the offer type and the expiration time.
Third is to drop of extra-ordinary visits, by using the interquartile range. But this one I have modified, because it dropped a bit too many records. I used 3 times the Interquartile range.

For the next Part I added some extra Information to the profiles. This includes the number of visits, the average amount spent and the standard deviation. This is bit biased but will work for my needs.


Required Installations:
[Numpy, Pandas, Matplotlib, kmodes, seaborn, datetime, json, math, scipy, sqlite3, sqlalchemy]
	All are listed in the requirements.txt file


Passing the values between ETL and ML-Part via the database (data.db)
I am Passing:
-	Portfolio
-	Transcript
-	Received Reaction Matrix
-	Profile 



2. 2   ML-Part:

First of all, to not get any errors, all columns and datatypes need to be redefined. 
Second, I implement the Feature Design:
 

—————————————————————————————————
Feature Design:

Using the transcript to get some extra Information about the proceed to fulfill the offer. 
First term – Interest in Offers – looks, on how many of the offers sent to the specific customer, and how many of those where viewed. I do add here a buffer, so that if 75 % of the Offers are viewed, in my eyes the person seams interested. And if 100 % of the Offers are viewed, I say this guy gets a bonus on this one.

Second term looks upon the specific payment. Fulfilling the offer with spending less than ‘normal’ can be considered as a coincidental fulfillment. Spending more than usual on the offer completion transaction, I would probably say that this extra amount is based on the attractiveness of the offer. Well since the median amount spent is biased by influenced Offers, this term can be discussed and may lead into an iterative process.
Again, there is a buffer at 75 %
 
Third term: ‘Reactiveness’ – High attractivity of an Offer results in a visit short time after realizing the offer. Completing withing the first quarter timewise till end of the offer, The Offer seams verry interesting. Later completions show less attractivity – so it is a decreasing function with growing time till completion. 

Out of those 3 I take the 2 higher values and multiply it with a definitely telling sign. If the Offer is seen and it is not completed at the first visit, it is clear that the offer not attractive. And if I continue this chain, if it is completed on a forth or fifth visit, it is by coincidence. Since this part is clear – and the others are more biased, it is outside the pre-choosing-process. 

Feature Evaluation is done by looking upon the values with the pandas describe tool.

Swell value to call an offer influenced can be set to the products of medians
—————————————————————————————————


2. 3   Clustering Part:

The Profiles need to be clustered. Using Principal Components Analyses, for the Age, Income, Member Since, Median Amount, Count and Standard Deviation cols. 

Hopkins Criteria signalizes a high tendency for Clusters.

For Choosing the right amount, I go through the Cost Function of Clustering (stepwidth: 2, Start 2 and end 2)

The elbow method leads to 8 Clusters. (2 + 3*2)



3. Results:


https://medium.com/@stefan.ludmann/the-quick-and-dirty-way-to-evaluate-offers-for-clusters-12f43f44dc06?source=friends_link&sk=045bed1e2c862dad072605f18da809de



4. Licensing, Authors and Acknowledgements

The Data is provided by Starbucks. Project Design by Udacity
Code written by Stefan Ludmann
