# python3 Q2.py --items=ml-100k/u.item ml-100k/u.data > Q2.txt

from mrjob.job import MRJob
from mrjob.step import MRStep
from io import open


class Movie(MRJob):

    def configure_args(self):
        super(Movie, self).configure_args()
        self.add_file_arg('--items', help='Path to u.item')

    def load_movie_names(self):
        # Load database of movie names.
        self.movieNames = {}
        with open("u.item", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   mapper_init = self.load_movie_names,
                   reducer=self.reducer2)]

    def mapper1(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield  movieID, (float(rating))

    def reducer1(self, movieID, value):
        values = []
        for v in value:
            values.append(v)
        countRating = len(values)
        avgRating = sum(values) / countRating
        if countRating > 100:
            yield (avgRating, countRating), movieID

    def mapper2(self, values, movieID):
        yield values, self.movieNames[int(movieID)]

    def reducer2(self, values, movies):
        for movie in movies:
            yield movie, values



if __name__ == '__main__':
    Movie.run()