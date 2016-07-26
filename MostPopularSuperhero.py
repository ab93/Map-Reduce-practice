from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperhero(MRJob):

    def configure_options(self):
        super(MostPopularSuperhero, self).configure_options()
        self.add_file_option('--names', help='Path to Marvel-Names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,
                   reducer_init=self.reducer_init, reducer=self.reducer_add_friends),
            MRStep(mapper=self.mapper_emit_value, reducer=self.reducer_get_maxcount)]

    def mapper_get_friends(self, __, line):
        line = line.split()
        character = line[0]
        num_friends = len(line) - 1
        yield character, num_friends

    def reducer_init(self):
        self.character_names = {}

        with open("Marvel-Names.txt") as f:
            for line in f:
                line = line.split()
                print line
                self.character_names[line[0]] = ' '.join(line[1:])

    def reducer_add_friends(self, key, value):
        name = self.character_names[key]
        yield name, sum(value)

    def mapper_emit_value(self, key, value):
        yield None, (value, key)

    def reducer_get_maxcount(self, key, value):
        yield max(value)


if __name__ == '__main__':
    MostPopularSuperhero.run()
