from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class MRMovieSimilarities(MRJob):
    def configure_options(self):
        super(MRMovieSimilarities, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')
        
    def load_movie_names(self):
        #load database of moview names, so we have movie names instead
        #of movie ID
        self.movieNames = {}
        
        with open("u.item") as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1].decode('utf-8',ignore)
                
    def step(self):
        return [
            MRStep(mapper = self.mapperRatingByUser,
                   reducer = self.reducerRatingByUser),
            MRStep(mapper = self.mapperMoviePair,
                  reducer = self.reducerCosineSimilarity),
            MRStep(mapper = self.mapperSortSimilarity,
                  reducer = self.reducerOutputSimilarity)
        ]
    def mapperRatingByUser(self, key, line):
        (userID, movieID, rating, timeStamp) = line.split('\t')
        yield userID, (movieID, float(rating))

    def reducerRatingByUser(self, userID, movieRatings):
        yield userID, movieRatings

    def mapperMoviePair(self, userID, movieRatings):
        for movieRatings1, movieRating2 in combinations(movieRatings,2):
            movieID1 = movieRatings1[0]
            rating1 = movieRatings1[1]
            movieID2 = movieRatings2[0]
            rating2 = movieRatings2[1]

            yield(movieID1, movieID2), (rating1, rating2)
            yield(movieID2, movieID1), (rating2, rating1)

    def cosineSimilarity(self, ratingPairs):
        numPairs = 0
        sumX = sumY = sumXY = 0

        for ratingX, ratingY in ratingPairs:
            sumX += ratingX * ratingX
            sumY += ratingY * ratingY
            sumXY += ratingX * ratingY
            numPairs += 1

        numerator = sumXY
        denominator = sqrt(sumX)*sqrt(sumY)
        score = 0
        if (denominator != 0):
            score = numerator / (float(denominator))
        return (score, numPairs)

    def reducerCosineSimilarity(self, moviePair, ratingPairs):

        score, numPairs = cosineSimilarity(raingPairs)
        #a little filter to ensure quality
        if (numPairs > 10 and score > 0.95):
            yield moviePair, (score, numPairs)

    def mapperSortSimilarity(self, moviePair, scores):
        score, n = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[int(movie1)], score),\
        (self.movieNames[int(movie2)], n)
    def reducerOutputSimilarity(self, movieScore, movieNumPair):
        movie1, score = movieScore
        for movie2, numPair in movieNumPair:
            yield movie1, (movie2, score, n)
                
if __name__ == "__main__":
    MRMovieSimilarities.run()
                
            