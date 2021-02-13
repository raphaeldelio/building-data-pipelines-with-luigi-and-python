import luigi
from luigi import Task, LocalTarget
from luigi.contrib.gcs import GCSTarget


class DownloadOrders(Task):
    def output(self):
        return LocalTarget('orders.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('To do...')


class DownloadSales(Task):
    def output(self):
        return LocalTarget('sales.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('To do...')


class DownloadInventory(Task):
    def output(self):
        return LocalTarget('inventory.csv')

    def run(self):
        with self.output().open('w') as f:
            f.write('To do...')


class UploadToGCS(Task):
    def requires(self):
        return [
            DownloadOrders(),
            DownloadSales(),
            DownloadInventory()
        ]

    def output(self):
        return GCSTarget('path')

    def run(self):
        with self.output().open('w') as f:
            f.write('To do...')


if __name__ == '__main__':
    luigi.run(['UploadToGCS', '--local-scheduler'])
