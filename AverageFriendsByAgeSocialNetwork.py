#MRJob is a python library with advanced sub-routines for Map and Reduce functionality
#We will use MRJob's mapper() and reducer() functions having respective massive data files as input
#We create lists of every column in the data file and use it for further processing
#Creating an Iterator over the input and yielding it is the preliminary story of Map and Reduce
#We setup the key field and fix it. Then iterate over those key fields and perform operations on others to get desired results
from mrjob.job import MRJob as mr


class AverageFriends(mr):
    
    #This is how we define mapper() function
    def mapper(self, _, Line):
    
    #Now we are going to initiate fields or columns of the file and convert them to lists based on how they are separated
    #In this case the data file is CSV (Comma Separated Value) file which means the values are separated by commas
        (Index, Name, Age, NumberOfFriends) = Line.split(",")
    
    #Now we are going to call yield for the method mapper() and the iterative output is caught by the reducer() function which will follow
    #We are looking for Age and Average NumberOfFriends so we just need to pass those two fields only
    #Here if you observe the NumberOfFriends value is in integer, so we need to cast it to float before we proceed
        FloatNumberOfFriends = float(NumberOfFriends)
        yield Age, FloatNumberOfFriends
        
    #Now it is time to define our reducer function and initiate or overload it with respective values from the mapper() function
    def reducer(self, AGE, NUMBER_OF_FRIENDS):
        
    #The formula is Sum(Total Number of Friends Age wise) then divide it by the Number of Friends Age wise
        Total_Number_Of_Friends_Agewise = sum(NUMBER_OF_FRIENDS)
        Number_of_Friends_Agewise = 0
        
    #Using for loop for populating Number_of_Friends_Agewise
        for count in NUMBER_OF_FRIENDS:
            Number_of_Friends_Agewise += 1
            
    #Now it is time to yield out results
        Average = round(Total_Number_Of_Friends_Agewise / Number_of_Friends_Agewise)
        yield AGE, Average
        
    #Now we are going to write two most common statments which will help interpreter to know about the file usage*
    # *That is, whether it is the main file or whether it is getting imported somewhere
if __name__ == '__main__':
    AverageFriends.run()     