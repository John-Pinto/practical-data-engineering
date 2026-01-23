import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser(description="Most popular listings")
parser.add_argument(
    "--listings_data_path",
    default="./data/airbnb-london-listings.csv.gz",
    help="Path to listings data",
)
parser.add_argument(
    "--reviews_data_path",
    default="./data/airbnb-london-reviews.csv.gz",
    help="Path to reviews data",
)
parser.add_argument(
    "--output_dir",
    default="./data/output/",
    help="Path to output directory",
)
args = parser.parse_args()


spark = (
    (SparkSession)  # SparkSession
    .builder.appName("Most Popular Listings")
    .getOrCreate()
)

listings_df = spark.read.csv(
    path=args.listings_data_path,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE",
)

reviews_df = spark.read.csv(
    path=args.reviews_data_path,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE",
)

listings_reviews_df = listings_df.join(
    reviews_df,
    on=listings_df.id == reviews_df.listing_id,
    how="inner",
)

reviews_per_listing_df = (
    (listings_reviews_df)
    .groupBy(
        listings_df.id,
        listings_df.name,
    )
    .agg(
        F.count(
            reviews_df.id,
        ).alias("review_count")
    )
    .orderBy("review_count", ascending=False)
)

(
    (reviews_per_listing_df)  # Output
    .write.csv(
        path=args.output_dir,
        header=True,
        mode="overwrite",
    )
)
