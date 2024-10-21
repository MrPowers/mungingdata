---
title: "Running Logistic Regressions with Spark"
date: "2018-05-15"
categories: 
  - "apache-spark"
---

# Running Logistic Regressions with Spark

Logistic regression models are a powerful way to predict binary outcomes (e.g. winning a game or surviving a shipwreck).

Multiple explanatory variables (aka "features") are used to train the model that predicts the outcome.

This episode shows how to train a Spark logistic regression model with the Titanic dataset and use the model to predict if a passenger survived or died.

We'll run our model on a test dataset and demonstrate that the model predicts the passenger survivorship accurately 83% of the time.

## Titanic Dataset

The `train.csv` file contains 891 rows of data in this schema:

```
PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S
3,1,3,"Heikkinen, Miss. Laina",female,26,0,0,STON/O2. 3101282,7.925,,S
5,0,3,"Allen, Mr. William Henry",male,35,0,0,373450,8.05,,S
6,0,3,"Moran, Mr. James",male,,0,0,330877,8.4583,,Q
7,0,1,"McCarthy, Mr. Timothy J",male,54,0,0,17463,51.8625,E46,S
8,0,3,"Palsson, Master. Gosta Leonard",male,2,3,1,349909,21.075,,S
```

- `PassengerId` is a unique identifier for each passenger
- `Survived` is `0` for passengers that died and `1` for passengers that survived
- `Pclass` is the ticket class from 1 - 3 (1 is the highest class, 2 is the middle class, and 3 is the lowest class)
- `Name` is the passenger's full name
- `Sex` is male or female
- `Age` is the passenger's age in years
- `SibSp` is the number of siblings / spouses on board
- `Parch` is the number of parents / children on board
- `Ticket` is the ticket number
- `Fare` is the passenger fare
- `Cabin` is the cabin number
- `Embarked` is the port where the passenger got on the ship

## Selecting Features

We need to select the explanatory variables that will be able to predict if passengers survived or died. It's always good to have a "plain English" reason why your explanatory variables are capable of predicting the outcome.

- `Gender` because females were more likely to be put on lifeboats
- `Age` because children were more likely to be saved
- `SibSp` because families we more likely to be saved together
- `Parch` because parent / children combinations were more likely to be saved
- `Fare` because the richer passengers were more likely to be saved

Explanatory variables are referred to as "features" in machine learning.

## Prepping the Training Dataset

We will create a `trainingDF()` method that returns a DataFrame with all of the features converted to floating point numbers, so they can be plugged into the machine learning model.

```
object TitanicData extends SparkSessionWrapper {

  def trainingDF(
    titanicDataDirName: String = "./src/test/resources/titanic/"
  ): DataFrame = {
    spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "train.csv")
      .withColumn(
        "Gender",
        when(
          col("Sex").equalTo("male"), 0
        )
          .when(col("Sex").equalTo("female"), 1)
          .otherwise(null)
      )
      .select(
        col("Gender").cast("double"),
        col("Survived").cast("double"),
        col("Pclass").cast("double"),
        col("Age").cast("double"),
        col("SibSp").cast("double"),
        col("Parch").cast("double"),
        col("Fare").cast("double")
      )
      .filter(
        col("Gender").isNotNull &&
          col("Survived").isNotNull &&
          col("Pclass").isNotNull &&
          col("Age").isNotNull &&
          col("SibSp").isNotNull &&
          col("Parch").isNotNull &&
          col("Fare").isNotNull
      )
  }

}
```

## Train the Model

Let's write a function that will convert all of the features into a single vector.

```
def withVectorizedFeatures(
  featureColNames: Array[String] = Array("Gender", "Age", "SibSp", "Parch", "Fare"),
  outputColName: String = "features"
)(df: DataFrame): DataFrame = {
  val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(featureColNames)
    .setOutputCol(outputColName)
  assembler.transform(df)
}
```

Now let's write a function that will convert the `Survived` column into a label.

```
def withLabel(
  inputColName: String = "Survived",
  outputColName: String = "label"
)(df: DataFrame) = {
  val labelIndexer: StringIndexer = new StringIndexer()
    .setInputCol(inputColName)
    .setOutputCol(outputColName)

  labelIndexer
    .fit(df)
    .transform(df)
}
```

We can train the model by vectorizing the features, adding a label, and fitting a logistic regression model with a DataFrame that has `feature` and `label` columns.

```
def model(df: DataFrame = TitanicData.trainingDF()): LogisticRegressionModel = {
  val trainFeatures: DataFrame = df
    .transform(withVectorizedFeatures())
    .transform(withLabel())
    .select("features", "label")

  new LogisticRegression()
    .fit(trainFeatures)
}
```

The model has coefficients and a y-intercept.

```
println(model().coefficients) // [2.5334201606150444,-0.021514292982670942,-0.40830426011779103,-0.23251735366607038,0.017246642519055992]

println(model().intercept) // -0.9761016366658759
```

## Evaluating Model Accuracy

Let's create a method that does all the data munging and return a properly formatted test dataset so we can run our logistic regresssion model.

```
object TitanicData extends SparkSessionWrapper {

  def testDF(
    titanicDataDirName: String = "./src/test/resources/titanic/"
  ): DataFrame = {
    val rawTestDF = spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "test.csv")

    val genderSubmissionDF = spark
      .read
      .option("header", "true")
      .csv(titanicDataDirName + "gender_submission.csv")

    rawTestDF
      .join(
        genderSubmissionDF,
        Seq("PassengerId")
      )
      .withColumn(
        "Gender",
        when(col("Sex").equalTo("male"), 0)
          .when(col("Sex").equalTo("female"), 1)
          .otherwise(null)
      )
      .select(
        col("Gender").cast("double"),
        col("Survived").cast("double"),
        col("Pclass").cast("double"),
        col("Age").cast("double"),
        col("SibSp").cast("double"),
        col("Parch").cast("double"),
        col("Fare").cast("double")
      )
      .filter(
        col("Gender").isNotNull &&
          col("Pclass").isNotNull &&
          col("Age").isNotNull &&
          col("SibSp").isNotNull &&
          col("Parch").isNotNull &&
          col("Fare").isNotNull
      )

  }

}
```

Just like we did with the training dataset, let's add one column with the vectorized features and another column with the label.

```
val testDF: DataFrame = TitanicData
  .testDF()
  .transform(withVectorizedFeatures())
  .transform(withLabel())
  .select("features", "label")
```

Now we're ready to run the logistic regression model.

```
val predictions: DataFrame = TitanicLogisticRegression
  .model()
  .transform(testDF)
  .select(
    col("label"),
    col("rawPrediction"),
    col("prediction")
  )
```

We can use the `BinaryClassificationEvaluator` class to test the accuracy of our model.

```
new BinaryClassificationEvaluator().evaluate(predictions) // 0.83
```

## Persisting the model

We can write the logistic regression model to disc with this command.

```
model().save("./tmp/titanic_model/")
```

The predictions can be generated with the model that's been persisted in the filesystem.

```
val predictions: DataFrame = LogisticRegressionModel
  .load("./tmp/titanic_model/")
  .transform(testDF)
  .select(
    col("label"),
    col("rawPrediction"),
    col("prediction")
  )
```

Training a model can be expensive and you'll want to persist your model rather than generating it on the fly every time it's needed.

## Next steps

Spark makes it easy to run logistic regression analyses at scale.

From a code organization standpoint, it's easier to separate the data munging and machine learning code in separate objects.

You'll need to understand how Spark executes programs to performance tune your models. Training a logistic regression model once and persisting the results will obviously be a lot faster than retraining the model every time it's run.
