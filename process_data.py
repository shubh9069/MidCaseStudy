def process_movie_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("MovieETL").getOrCreate()
    # Load data from CSV
    source_path = "/Hydra-Movie-Scrape.csv"
    movies_df = spark.read.csv(source_path, header=True, inferSchema=True)


    # i) Which movies were released in the year 2020?
    movies_2020 = movies_df.filter(year("Release Year") == 2020)

    # ii) What is the average IMDb rating of the movies in the dataset?
    average_rating = movies_df.select(avg("IMDB Rating")).collect()[0][0]

    # iii) Which movies have the longest and shortest runtimes?
    longest_runtime_movie = movies_df.filter(col("Runtime") == max("Runtime")).select("Movie Title", "Runtime").collect()[0]
    shortest_runtime_movie = movies_df.filter(col("Runtime") == min("Runtime")).select("Movie Title", "Runtime").collect()[0]


    # iv) How many movies were directed by each director?
    movies_count_by_director = movies_df.groupBy("Directors").agg(count("*").alias("Movie Count"))

    # v) Who are the unique writers in the dataset?
    unique_writers = movies_df.select("Writers").distinct()

    # vi) Which movies have an IMDb rating greater than 8.0?
    high_rated_movies = movies_df.filter(col("IMDB Rating") > 8.0)

    # vii) Which movies do not have a YouTube trailer code?
    movies_without_trailer = movies_df.filter(col("YouTube Trailer Code").isNull())

    # viii) How many movies does each cast member appear in?
    movies_per_cast_member = movies_df.select("Cast").groupBy("Cast").agg(count("*").alias("Movie Count"))
    # Display or save the results as needed
    # ...
    # Stop the Spark session
    spark.stop()