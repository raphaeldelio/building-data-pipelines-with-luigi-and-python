import luigi


class SayHello(luigi.Task):
    def output(self):
        return luigi.LocalTarget('result.csv')

    def run(self):
        print('hello world')
        with self.output().open('w') as f:
            f.write('ok')


if __name__ == '__main__':
    luigi.run(['SayHello', '--local-scheduler'])
