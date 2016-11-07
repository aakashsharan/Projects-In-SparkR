#check the system enviroment variables for spark home.
Sys.getenv()

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g"))

sqlContext <- sparkRSQL.init(sc)

df_r <- read.csv("census.csv")
census <- createDataFrame(sqlContext, df_r)
nrow(census)
showDF(census)
printSchema(census)

# seed is set internally
trainingData <- sample(census, FALSE, 0.6)
nrow(trainingData)
testData <- except(census, trainingData)
nrow(testData)
#print(count(trainingData))
#head(trainingData)
#head(testData)

# lets train a logistic regression model with family binomial
regModel <- glm(over50k ~ age + workclass + education + maritalstatus + occupation + race + sex + hoursperweek, data = trainingData, family = "binomial")
summary(regModel)

predictionsRegModel <- predict(regModel, newData = testData)
showDF(select(predictionsRegModel, "label", "prediction"))
#showDF(predictionsRegModel)

# Evaluate Logistic Regression model
errorsLogR <- select(predictionsRegModel, predictionsRegModel$label, predictionsRegModel$prediction, alias(abs(predictionsRegModel$label - predictionsRegModel$prediction), "error"))
showDF(errorsLogR)
