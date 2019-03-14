import csv

def create_csv(filename,listname):
	the_file = open(filename,'w')
	writer = csv.writer(the_file)
	for row in listname:
		writer.writerow(row)
	the_file.close



