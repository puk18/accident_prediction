#!/usr/bin/env python
from preprocess import get_negative_samples, get_positive_samples
from utils import init_spark
from preprocess import get_dataset_df
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, \
                              CrossValidator
from pyspark.ml import Pipeline
from random_undersampler import RandomUnderSampler
from random_forest import get_feature_importances
from export_results import *

result_dir = create_result_dir('urf')
spark = init_spark()


neg_samples = get_negative_samples(spark).sample(0.5)

pos_samples = get_positive_samples(spark)
print(pos_samples.count())
print(neg_samples.count())





imbalance_ratio = (neg_samples.count()/pos_samples.count())
print(imbalance_ratio)

train_set, test_set = get_dataset_df(spark, pos_samples, neg_samples)
train_set, test_set = train_set.persist(), test_set.persist()

rf = RandomForestClassifier(labelCol="label",
                           featuresCol="features",
                           cacheNodeIds=True,
                           maxDepth=17,
                           impurity='entropy',
                           featureSubsetStrategy='sqrt',
                           minInstancesPerNode=10,
                           numTrees=100,
                           subsamplingRate=1.0,
                           maxMemoryInMB=768)
ru = (RandomUnderSampler()
     .setIndexCol('sample_id')
     .setTargetImbalanceRatio(1.0))
pipeline = Pipeline().setStages([ru, rf])
model = pipeline.fit(train_set)
predictions = model.transform(test_set).persist()
train_predictions = model.transform(train_set).persist()

write_params(model, neg_samples.count(),result_dir)
write_results(predictions, train_predictions, result_dir)

# Write feature importances
feature_importances = get_feature_importances(model.stages[1])
feature_importances.to_csv(result_dir + '/feature_importances.csv')
