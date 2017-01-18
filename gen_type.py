import csv

def readtypes():
    with open('type.csv', 'r') as f:
        for line in f:
            print line.strip(),
            print ', text'

def typegenerator():
    container = dict()
    with open('type.csv') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            container[row[0].strip()] = row[1].strip()
    return container

if __name__ == "__main__":
    temp = typegenerator()
    print temp