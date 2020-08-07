from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import TrainValidationSplit, \
                              ParamGridBuilder, \
                              CrossValidator
from pyspark.ml import Pipeline
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col, floor, sum, when
import pandas as pd
import numpy as np
from preprocess import features_col
from random_undersampler import RandomUnderSampler


def random_forest_tuning(train_samples):
    rf = RandomForestClassifier(labelCol="label",
                                featuresCol="features",
                                cacheNodeIds=True)
    ru = RandomUnderSampler().setIndexCol('sample_id')
    pipeline = Pipeline().setStages([ru, rf])
    paramGrid = \
        (ParamGridBuilder()
         .addGrid(rf.numTrees, [50, 75, 100])
         .addGrid(rf.featureSubsetStrategy, ['sqrt'])
         .addGrid(rf.impurity, ['gini', 'entropy'])
         .addGrid(rf.maxDepth, [5, 15, 30])
         .addGrid(rf.minInstancesPerNode, [1])
         .addGrid(rf.subsamplingRate, [1.0, 0.6, 0.4])
         .addGrid(ru.targetImbalanceRatio, [1.0, 1.5, 2.0])
         .build())
    pr_evaluator = \
        BinaryClassificationEvaluator(labelCol="label",
                                      rawPredictionCol="rawPrediction",
                                      metricName="areaUnderPR")
    tvs = TrainValidationSplit(estimator=pipeline,
                               estimatorParamMaps=paramGrid,
                               evaluator=pr_evaluator,
                               trainRatio=0.8,
                               collectSubModels=True)

    model = tvs.fit(train_samples)

    return model





def compute_precision_recall(predictions, threshold):
    def prob_positive(v):
        try:
            return float(v[1])
        except ValueError:
            return None

    prob_positive_udf = udf(prob_positive, DoubleType())
    true_positive = (predictions
                     .select('label', 'prediction')
                     .filter((col('label') == 1.0)
                             & (prob_positive_udf('probability') > threshold))
                     .count())
    false_positive = (predictions
                      .select('label', 'prediction')
                      .filter((col('label') == 0.0)
                              & (prob_positive_udf('probability') > threshold))
                      .count())
    true_negative = (predictions
                     .select('label', 'prediction')
                     .filter((col('label') == 0.0)
                             & (prob_positive_udf('probability') < threshold))
                     .count())
    false_negative = (predictions
                      .select('label', 'prediction')
                      .filter((col('label') == 1.0)
                              & (prob_positive_udf('probability') < threshold))
                      .count())
    try:
        precision = true_positive / (true_positive + false_positive)
    except ZeroDivisionError:
        precision = None
    try:
        recall = true_positive / (true_positive + false_negative)
    except ZeroDivisionError:
        recall = None
    return (precision, recall)


def compute_precision_recall_graph_slow(predictions, n_points):
    def gen_row(threshold):
        result = compute_precision_recall(predictions, threshold)
        return (threshold, result[0], result[1])

    space = np.linspace(0, 1, n_points)
    graph = pd.DataFrame([gen_row(t) for t in space],
                         columns=['Threshold', 'Precision', 'Recall'])

    return graph


def compute_threshold_dependent_metrics(spark, predictions, n_points):
    inf_cumulative_window = \
        (Window
         .partitionBy('label')
         .orderBy('id_bucket')
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    sup_cumulative_window = \
        (Window
         .partitionBy('label')
         .orderBy('id_bucket')
         .rowsBetween(1, Window.unboundedFollowing))

    def prob_positive(v):
        try:
            return float(v[1])
        except ValueError:
            return None

    prob_positive = udf(prob_positive, DoubleType())
    count_examples = predictions.count()
    id_buckets = spark.createDataFrame(zip(range(-1, n_points),),
                                       ['id_bucket'])
    label = spark.createDataFrame([(0,), (1,)], ['label'])

    return \
        (predictions
         .select('label',
                 floor(prob_positive('probability') * n_points)
                 .alias('id_bucket'))
         .groupBy('label', 'id_bucket').count()
         .join(id_buckets.crossJoin(label), ['id_bucket', 'label'], 'outer')
         .na.fill(0)
         .withColumn('count_negatives',
                     sum('count').over(inf_cumulative_window))
         .withColumn('count_positives',
                     sum('count').over(sup_cumulative_window))
         .groupBy('id_bucket').pivot('label', [0, 1])
         .sum('count_negatives', 'count_positives')
         .select(((col('id_bucket') + 1) / n_points).alias('Threshold'),
                 col('0_sum(count_negatives)').alias('true_negative'),
                 col('0_sum(count_positives)').alias('false_positive'),
                 col('1_sum(count_negatives)').alias('false_negative'),
                 col('1_sum(count_positives)').alias('true_positive'))
         .na.fill(0)
         .withColumn('Precision',
                     col('true_positive')
                     / (col('true_positive') + col('false_positive')))
         .withColumn('Recall',
                     when(col('true_positive') != 0,
                          col('true_positive')
                          / (col('true_positive') + col('false_negative')))
                     .otherwise(0.0))
         .withColumn('False positive rate',
                     col('false_positive')
                     / (col('false_positive') + col('true_negative')))
         .withColumn('Accuracy',
                     (col('true_positive') + col('true_negative'))
                     / (col('true_positive') + col('true_negative')
                        + col('false_positive') + col('false_negative')))
         .withColumn('F1 Score',
                     (2 * col('Precision') * col('Recall'))
                     / (col('Precision') + col('Recall')))
         .withColumn('True negative percentage',
                     col('true_negative') / count_examples)
         .withColumn('True positive percentage',
                     col('true_positive') / count_examples)
         .withColumn('False negative percentage',
                     col('false_negative') / count_examples)
         .withColumn('False positive percentage',
                     col('false_positive') / count_examples)
         .drop('true_negative', 'true_positive',
               'false_positive', 'false_negative')
         .orderBy('Threshold')
         .toPandas())


def get_feature_importances(model):
    feature_importances = pd.DataFrame(model.featureImportances.toArray())
    feature_importances.index = features_col
    feature_importances.columns = ["Feature importances"]
    feature_importances = \
        feature_importances.sort_values(['Feature importances'],
                                        ascending=False)
    return feature_importances
