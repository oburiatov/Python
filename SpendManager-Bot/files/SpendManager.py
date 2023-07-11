class SpendManager:
    expenseName= "NULL"
    expenseValue= "NULL"
    expenseHashtag= "NULL"

    def setExpenseName(self,value):
        self.expenseName=value

    def setExpenseValue(self,value):
        self.expenseValue=value

    def setExpenseHashtag(self,value):
        self.expenseHashtag=value
    
    def getExpenseName(self):
        return self.expenseName
    
    def getExpenseValue(self):
        return self.expenseValue
    
    def getExpenseHashtag(self):
        return self.expenseHashtag
