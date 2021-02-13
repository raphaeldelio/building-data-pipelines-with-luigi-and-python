import luigi
from luigi import Task, LocalTarget


class ProcessOrders(Task):
    def output(self):
        return LocalTarget('orders.csv')

    def run(self):
        with self.output().open('w') as f:
            print('May,100', file=f)
            print('May,150', file=f)
            print('June,140', file=f)
            print('June,1500', file=f)


class GenerateReport(Task):
    def requires(self):
        return ProcessOrders()

    def output(self):
        return LocalTarget('result.csv')

    def run(self):
        report = {}
        for line in self.input().open():
            month, value = line.split(',')

            if month in report:
                report[month] += float(value)
            else:
                report[month] = float(value)

        with self.output().open('w') as out:
            for month in report:
                print(month + ',' + str(report[month]), file=out)


class SummarizeReport(Task):
    def requires(self):
        return GenerateReport()

    def output(self):
        return LocalTarget('summary.txt')

    def run(self):
        total = 0.0
        for line in self.input().open():
            month, value = line.split(',')
            total += float(value)

        with self.output().open('w') as out:
            out.write(str(total))


if __name__ == '__main__':
    luigi.run(['SummarizeReport'])
