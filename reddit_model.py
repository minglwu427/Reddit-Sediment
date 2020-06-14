from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# IMPORT OTHER MODULES HERE
from pyspark.sql.functions import udf
import cleantext 
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType

# Bunch of imports (may need more)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
 




def main(context):
    """Main function takes a Spark SQL context."""
    """
    comments = context.read.json("comments-minimal.json")
    #submissions = context.read.json("submissions.json.bz2")
    labels = context.read.csv("labeled_data.csv", header = "true", inferSchema = "true")

    
    comments.write.mode('overwrite').parquet("comments")
    #submissions.write.parquet("submissions")
    #labels.write.parquet("labels")
    
    #Task1
    comments = context.read.parquet("comments")
    submissions = context.read.parquet("submissions")
    #labels = context.read.parquet("labels", header = True)
    
    #Task2
    
    labels.createTempView("label_view")
    comments.createTempView("comments_view")
    submissions.createTempView("submissions_view")
    modeling_data = context.sql('SELECT c.id, body FROM comments_view as c inner JOIN label_view as l on c.id = l.Input_id')


    #modeling_data.show()
    """
    """
    
    #Task4
    """
    """
    cleantxt_udf = udf(cleantext.sanitize,ArrayType(ArrayType(StringType())))
    modeling_data = modeling_data.select('id',cleantxt_udf('body').alias('SanitizeS'))
    #modeling_data.show(n=1,truncate = False)

    
    #Task5

    concatgrams_udf = udf(concatgrams, ArrayType(StringType()))
    temp = concatgrams_udf('SanitizeS').alias('grams')
    modeling_data = modeling_data.select('id',temp)
    #modeling_data.show(n=3)
    
    #Task6A 
    cv = CountVectorizer(inputCol="grams", outputCol="features", vocabSize=1 << 18 ,minDF=5.0, binary=True)
    model = cv.fit(modeling_data)
    result =  model.transform(modeling_data)
    #result.show(n=5,truncate=False)
    result.createTempView("result_view")
    """
    #Task6B
    '''
    labels_data = context.sql("""
                              select Input_id,
                              CASE when labeldjt=1 then 1 else 0 end as pdjt,
                              CASE when labeldjt=-1 then 1 else 0 end as ndjt,
                              CASE when labeldem=1 then 1 else 0 end as pdem,
                              CASE when labeldem=-1 then 1 else 0 end as ndem,
                              CASE when labelgop=1 then 1 else 0 end as pgop,
                              CASE when labelgop=-1 then 1 else 0 end as ngop,
                              features
                              from label_view as l 
                              inner JOIN result_view as r on r.id = l.Input_id
                              """)
    #modeling_data.show(n=1, truncate = False)
    
    #Task7 
    
    labels_data.createTempView("fdata_view")
    p_data = context.sql("""
                         select input_id, pdjt as label,features 
                         from fdata_view
                         """)
    
    n_data = context.sql("""
                         select input_id, ndjt as label,features 
                         from fdata_view
                         """)
    
    
        
    
    # Initialize two logistic regression models.
    # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
    poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    poslr.setThreshold(0.2)
    neglr.setThreshold(0.25)
    # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
    posEvaluator = BinaryClassificationEvaluator()
    negEvaluator = BinaryClassificationEvaluator()
    # There are a few parameters associated with logistic regression. We do not know what they are a priori.
    # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
    # We will assume the parameter is 1.0. Grid search takes forever.
    posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
    negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
    # We initialize a 5 fold cross-validation pipeline.
    posCrossval = CrossValidator(
        estimator=poslr,
        evaluator=posEvaluator,
        estimatorParamMaps=posParamGrid,
        numFolds=2)
    negCrossval = CrossValidator(
        estimator=neglr,
        evaluator=negEvaluator,
        estimatorParamMaps=negParamGrid,
        numFolds=2)
    # Although crossvalidation creates its own train/test sets for
    # tuning, we still need a labeled test set, because it is not
    # accessible from the crossvalidator (argh!)
    # Split the data 50/50
    posTrain, posTest = p_data.randomSplit([0.5, 0.5])
    negTrain, negTest = n_data.randomSplit([0.5, 0.5])
    # Train the models
    print("Training positive classifier...")
    posModel = posCrossval.fit(posTrain)
    print("Training negative classifier...")
    negModel = negCrossval.fit(negTrain)
    
    # Once we train the models, we don't want to do it again. We can save the models and load them again later.
    posModel.write().overwrite().save("www/pos.model")
    negModel.write().overwrite().save("www/neg.model")
    '''
    # Task8
    """
    test_data  = context.sql('''SELECT c.id, c.created_utc, s.score as sub_score, s.title, c.score as com_score, c.body, c.author_flair_text as states
                        from comments_view as c
                        INNER JOIN submissions_view as s
                        on s.id = RIGHT(c.link_id, LENGTH(c.link_id) - 3) 
                        WHERE c.id NOT IN (SELECT id FROM comments_view WHERE body like '%/s%' AND body like '&gt%')
                        ''')
    test_data = test_data.sample(False, 0.1,None)
    cleantxt_udf = udf(cleantext.sanitize,ArrayType(ArrayType(StringType())))
    test_data = test_data.select('id', 'created_utc', 'sub_score', 'com_score'
                                 , 'title',cleantxt_udf('body').alias('SanitizeS'),'states')
    
    
    concatgrams_udf = udf(concatgrams, ArrayType(StringType()))
    temp = concatgrams_udf('SanitizeS').alias('grams')
    test_data = test_data.select('id', 'sub_score', 'com_score','created_utc', 'title',temp, 'states')
    
    model = cv.fit(modeling_data)
    test_data =  model.transform(test_data)
    
    test_data.write.mode('overwrite').parquet("test_data")
    """
    posModel = CrossValidatorModel.load('www/pos.model')
    negModel = CrossValidatorModel.load('www/neg.model')
    
    #Task 9
    test_data =  context.read.parquet("test_data")
    test_data.show()

    
    posResult = posModel.transform(test_data)
    posResult.show()
    posResult = posResult.select('id' , 'features','sub_score', 'com_score','created_utc' , 'title', 'states', posResult["prediction"].alias("p_pred"))
    negResult = negModel.transform(posResult)
    negResult = negResult.select('id' ,'created_utc' , 'sub_score', 'com_score','title', 'states',"p_pred",negResult['prediction'].alias('n_pred'))
    negResult.createTempView('final_res')
    #negResult = context.sql('select * from negResult_view')
    negResult.show()
    negResult.write.mode('overwrite').parquet("final_result")
    
    #Task10
    
 
    
    states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 
          'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 
          'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 
          'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 
          'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 
          'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 
          'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 
          'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 
          'Wisconsin', 'Wyoming']
    

    
    
    t1 = context.sql('''
                    select title, sum(p_pred)/count(p_pred) as positp, sum(n_pred)/count(n_pred) as negatp
                    from final_res
                    group by title
                ''')
    t1.write.mode('overwrite').csv('%_by_comments.csv',header = True)



    t2 = context.sql('''
                    select FROM_UNIXTIME(created_utc, 'yyyy-MM-dd') as date,  sum(p_pred)/count(p_pred) as positp, sum(n_pred)/count(n_pred) as negatp
                    from final_res
                    group by date
                    ''')

    t2.write.mode('overwrite').csv('%_by_date',header = True)
    
    t3 = context.sql('''
                    select states,  sum(p_pred)/count(p_pred) as positp, sum(n_pred)/count(n_pred) as negatp
                    from final_res
                    group by states
                    ''')

    t3 = t3.select('states','positp', 'negatp').where(t3.states.isin(states))
    t3.show()
    t3.write.mode('overwrite').csv('%_by_states', header = True)
    
    
    f_result = context.read.parquet("final_result")
    f_result.createTempView('final_res')
    
    f_result.show()    
    
    
    t1_result = context.read.csv("%_by_comments.csv")
    t2_result = context.read.csv("%_by_date")
    t3_result = context.read.csv("%_by_states")

    
    
    
    t1_result.printSchema()
    t2_result.printSchema()
    t3_result.printSchema()
    

    t1_result.show()
    t2_result.show()
    t3_result.show()
    # YOUR CODE HERE
    # YOU MAY ADD OTHER FUNCTIONS AS NEEDED
    
    
    
def concatgrams(result):
    final_array= []
    for grams in result:
        final = grams.split()
        for word in final:
            #final_array.append("\"" + word + "\"")
            final_array.append(word)
    return final_array
        

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

