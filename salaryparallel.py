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

class CountryCount(Task):
    def requires(self):
        return GenerateData()
    def output(self):
        return LocalTarget("countrycount.csv")
    def run(self):
        cntry = {}
        for line in self.input().open():
            cols = line.split(',')
            country = cols[4].strip()
            if country in cntry:
                cntry[country] += 1
            else:
                cntry[country] = 1

        with self.output().open('w') as f:
            for c in cntry:
                print("{},{}".format(c, cntry[c]), file=f)

class AverageAge(Task):
    def requires(self):
        return GenerateData()
    def output(self):
        return LocalTarget("age.csv")
    def run(self):
        emp = {}
        total = 0.0
        count = 0
        for line in self.input().open():
            cols = line.split(',')
            if cols[0] not in emp:
                emp[cols[0]] = 0
                total += int(cols[1])
                count += 1

        with self.output().open('w') as f:
            f.write(str(total / count))

class SummarizeReport(Task):
    def requires(self):
        return [ProcessSalaries(),
                CountryCount(),
                AverageAge()]
    def output(self):
        return LocalTarget("summary.txt")
    def run(self):
        total_salary = 0.0
        average_age = 0.0
        for line in self.input()[0].open():
            month, amount = line.split(',')
            total_salary += float(amount)

        for line in self.input()[2].open():
            average_age = float(line)

        with self.output().open('w') as f:
            print("Total salary payout is {}".format(total_salary), file=f)
            print("Average age is {}".format(average_age), file=f)

            for line in self.input()[1].open():
                country, count = line.split(',')
                print("Total {} employees in {}".format(count.strip(),country), file=f)

if __name__ == '__main__':
    luigi.run(['SummarizeReport','--local-scheduler'])
