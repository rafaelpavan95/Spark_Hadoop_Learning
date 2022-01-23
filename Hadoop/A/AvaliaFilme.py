from mrjob.job import MRJob



class MRAvaliaFilme(MRJob):

    def mapper(self, key, line):

        (userID, movieID, rating, timestamp) = line.split('\t') # separa coluna de cada linha

        yield rating, 1 

        # generator, itera sem ocupar espaço na memória

        # rating é palavra chave, cada valor é mapeado utilizando o valor 1

        # ex: 1,2,3,4,5

        # saída 1:1, 2:1, 3:1, 4:1, 5:1

    def reducer(self, rating, occurences):
        
        yield rating, sum(occurences)


if __name__ == '__main__':
    MRAvaliaFilme.run()


