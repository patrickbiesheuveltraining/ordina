import luigi
from luigi import Task, LocalTarget

class GenerateData(Task):
    def output(self):
        return LocalTarget('input.csv')

    def run(self):
        with self.output().open('w') as f:
            #name, age, month, salary, country
            print('Emma,27,6,500,Sweden', file=f)
            print('Emma,27,7, 600,Sweden', file=f)
            print('August,29,6,2000,Sweden', file=f)
            ....
class ProcessSalaries(Task):
    def requires(self):
        return GenerateData()
    def output(self):
        return LocalTarget("salaries.csv")
    def run(self):
        emp ={}
        for line in self.input().open():
            cols = line.split(',')
            if cols[0] in emp:
                emp[cols[0]] += int(cols[3])
            else:
                emp[cols[0]] = int(cols[3])
        print(emp)
        with self.output().open('w') as f:
            for e in emp:
                print("{},{}".format(e,emp[e]), file=f)

class SummarizeReport(Task):
    def requires(self):
        return ProcessSalaries()
    def output(self):
        return LocalTarget("summary.txt")
    def run(self):
        total = 0.0
        for line in self.input().open():
            month, amount = line.split(',')
            total += float(amount)
        with self.output().open('w') as f:
            f.write(str(total))

if __name__ == '__main__':
    luigi.run(['SummarizeReport','--local-scheduler'])
