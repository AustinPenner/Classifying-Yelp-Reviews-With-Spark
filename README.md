# Classifying Yelp Reviews With Spark

As a pizza enthusiast, I'm always combing through online reviews looking for new restaurants to try. While looking through reviews I noticed a stark difference in how low-star reviews were written vs high-star reviews. Inspired, I wanted to analyze the relationship between the content of a review and the rating a user gave.

### What sort of analysis?

I used NLP on Yelp review data to look at a review's text and predict whether it was a positive or negative review. Reviews can have values of 1 to 5 stars, which I bucketed into 4-5 stars for a positive review, and 1-3 stars for a negative review. The reason for this is that most people use online reviews this way (most do not consider a 3.2 star rating to be good). The classes are relatively balanced at 65% positive to 35% negative.

![alt text](https://github.com/AustinPenner/Classifying-Yelp-Reviews-With-Spark/blob/master/images/Ratings%20Distribution.png "Ratings Distribution")

### What about the data?

The dataset consists of 6.7 million reviews comprising 5GB of data. Approximately 70% of business were in the US and virtually all the remainder were in Canada. About 34% of the businesses were restaurants while shopping, bars/nightlife, and various others made up the rest. The average review was ~110 words before preprocessing. 

### Cluster computing - pretty simple right?

Not quite. 

I could have utilized more straightforward tools like Pandas and sklearn for my analysis, but instead chose to work with Spark because it's the industry standard for working with large datasets. There were a number of learning opportunities in setting up an EMR cluster and working with pyspark; I learned how to choose how many and what kind of AWS nodes to use for my NLP task, and I learned how to import data within Spark and utilize its specialized distributed-computing tools.

For my analysis I used 16 r5.xlarge nodes (each with 4 vCPUs and 32GB RAM) to do NLP preprocessing and train a model. I utilized many built-in Spark classes including HashingTF, IDF, and RandomForestClassifier. See the code for more detail.

### Methods and Results

Text vectorization:
* Tokenized by word
* Removed stop words and punctuation
* Stemmed words with NLTK's SnowballStemmer
* Filtered on reviews with 10 or more words
* Vectorized using TF-IDF

The resulting matrix had 260,000 features. I tried Naive Bayes for a baseline before landing on Random Forest as the best-performing model, despite its slow train speed. Based only on review text across multiple business types the model had a classification accuracy of **88%**.

___

#### Technologies

* Python
* Spark
* Cluster computing (Amazon EMR)
* MongoDB
* S3 (Amazon storage)
