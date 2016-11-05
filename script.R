#check the system enviroment variables for spark home.
Sys.getenv()
library(ggplot2)
library(plotly)

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sc <- sparkR.init(master = "local[*]", sparkEnvir = list(spark.driver.memory="2g", sparkPackages="com.databricks:spark-csv_2.11:1.0.3"))

sqlContext <- sparkRSQL.init(sc)
# R dataframe
df_o <- read.csv("Iris.csv")
# convert R dataframe to SparkR DataFrame
df <- createDataFrame(sqlContext, df_o)
head(df)

# select and filter operations.
head(select(df, df$SepalLengthCm, df$Species))
head(filter(df, df$SepalLengthCm > 5.0))

# Compute average PetalLengthCm and group by Species.
head(agg(groupBy(df, "Species"), PetalLengthCm="avg"))

# Returns the schema of this DataFrame as a structType object.
dfSchema <- schema(df)
dfSchema

# Sort the DataFrame by the specified column.
head(arrange(df, df$SepalLengthCm))
# Sort in decreasing order
head(arrange(df, "SepalLengthCm", decreasing = TRUE))
#Print the first numRows rows of a DataFrame
showDF(df)

# Running SQL Queries from SparkR
registerTempTable(df, "iris")
irisSepalLGreater5 <- sql(sqlContext, "SELECT Id, Species FROM iris WHERE SepalLengthCm > 5.0")
head(irisSepalLGreater5)

# lets draw some cool graphs with ggplotly
# PetalLength and PetalWidth variation against each other
plot1 <- ggplot(df_o , aes(PetalLengthCm, PetalWidthCm)) + geom_point(size = 1)
# PetalLength and PetalWidth variation against each other within each Species?
plot2 <- ggplot(df_o, aes(PetalLengthCm, PetalWidthCm, color = Species)) + geom_point(size=1)
ggplotly(plot1)
ggplotly(plot2)
