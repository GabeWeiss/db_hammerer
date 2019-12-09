import re

print ("Proof of concept for Cloud Next 2020 relational database showcase demo\n")
print ("Time to pick your workload. Enter the number corresponding to your selection.")

def ask_for_read_workload():
    return input("1) Consistently high reads\n2) Consistent but slow reads\n3) Spikey reads\nWhat type of read traffic do you want? ")

def ask_for_write_workload():
    return input("1) Consistently high writes\n2) Consistent but slow writes\n3) Spikey writes\nWhat type of write traffic do you want? ")

def ask_for_database():
    return input("1) Cloud SQL\n2) Spanner\n3) ??? (for PoC only Cloud SQL will work) ")
read_workload = ask_for_read_workload()

while re.match('[^1-3]+', read_workload):
    print ("\nYour selection for read traffic didn't match one of the options.\n")
    read_workload = ask_for_read_workload()

write_workload = ask_for_write_workload()

while re.match('[^1-3]+', write_workload):
    print ("\nYour selection for write traffic didn't match one of the options.\n")
    write_workload = ask_for_write_workload()

print ("\nWhich database should we use?\n")

database_choice = ask_for_database()
while database_choice != "1":
    print ("Sorry, only Cloud SQL is available for the proof of concept.")
    database_choice = ask_for_database()

