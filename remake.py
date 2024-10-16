import sqlite3
import pandas as pd
# read_sql, to_sql -не являются частью класса DataFrame

class siteUpload:
    def __init__(self, excelFile='', csvFile='',connector='', siteTable='', shopTable=''):
        self.excelFile = excelFile
        self.csvFile = csvFile
        self.connector = connector
        self.siteTable = siteTable
        self.shopTable = shopTable

    def exceltoCsv(self):
        excelFile = pd.read_excel(self.excelFile,header=0,na_values=False)
        excelFile.to_csv(self.csvFile,sep=';',index=None,header=True,encoding='UTF-8')
    
    def csvTosql(self):
        dataFrame = pd.read_csv(self.csvFile,delimiter=';',header=0,encoding='UTF-8',na_values=False)
        dataFrame.to_sql(self.shopTable,self.connector,if_exists='replace',index=False)

    def updateCommonProducts(self):
        sql_query = f'''UPDATE {self.siteTable} 
                SET Quantity = U."Кол-во",
                Price = U."Продажная цена"
                FROM (SELECT {self.shopTable}.Штрихкод, {self.shopTable}."Кол-во", {self.shopTable}."Продажная цена" FROM {self.shopTable}) AS U
                WHERE SKU = U.Штрихкод'''
        cur = self.connector.cursor()
        cur.execute(sql_query)
        self.connector.commit()
        print(f'Number of rows affected: {cur.rowcount}')

    def getCommonProducts(self):
        sql_query = f'SELECT * FROM {self.siteTable} WHERE SKU IN (SELECT Штрихкод FROM {self.shopTable})'
        updatedProducts = pd.read_sql(sql_query, self.connector)
        outputFile=str('updated_'+ self.csvFile)
        updatedProducts.to_csv(outputFile,sep=';',index=None,header=True,encoding='UTF-8')

    def getDifferProducts(self):
        sql_query = f'''SELECT Штрихкод AS SKU, 
        CASE 
        WHEN Категория = "бокс" THEN "Боксы"
        WHEN Категория = "гель для душа" THEN "Гели для душа"
        WHEN Категория = "готовые продукты" THEN "Готовые продукты"
        WHEN Категория = "зубные пасты" THEN "Зубные пасты"
        WHEN Категория = "косметика" THEN "Косметика"
        WHEN Категория = "Лапша" THEN "Рамены"
        WHEN Категория = "латиао" THEN "Латиао"
        WHEN Категория = "напитки" THEN "Напитки"
        WHEN Категория = "Приправа" THEN "Приправы"
        WHEN Категория = "сладости" THEN "Сладости"
        WHEN Категория = "СОУС ПАСТА" THEN "Соусы"
        WHEN Категория = "сухари панировочные" THEN "Сухари панировочные"
        WHEN Категория = "Токпокии" THEN "Токпокки"
        WHEN Категория = "химия" THEN "Бытовая химия"
        WHEN Категория = "шампуни" THEN "Шампуни"
        ELSE Категория
        END Category,
        {self.shopTable}."Название товара " AS Title,
        {self.shopTable}."Продажная цена" AS Price,
        {self.shopTable}."Кол-во" AS Quantity
        FROM {self.shopTable} WHERE Штрихкод NOT IN(SELECT SKU FROM {self.siteTable} WHERE SKU IS NOT NULL)'''
        diffProducts = pd.read_sql(sql_query, self.connector)
        outputFile = str('diff_' + self.csvFile)
        diffProducts.to_csv(outputFile,sep=';',index=None,header=True,encoding='UTF-8')
    
    def makeFullupload(self):
        self.exceltoCsv()
        self.csvTosql()
        self.updateCommonProducts()
        self.getCommonProducts()
        self.getDifferProducts()
def update_All():
    upload_Uralsk=siteUpload('Uralsk.xlsx','Uralsk.csv',connector,'Tilda_Uralsk','Uralsk')
    upload_Uralsk.makeFullupload()
    upload_Atyrau = siteUpload('Atyrau.xlsx','Atyrau.csv',connector,'Tilda_Atyrau','Atyrau')
    upload_Atyrau.makeFullupload()
def append_All():
    dataFrame_Atyrau = pd.read_excel('append_Atyrau.xlsx',header=0,na_values=False)
    dataFrame_Atyrau.to_sql('Tilda_Atyrau',connector,if_exists='append',index=False)
    dataFrame_Uralsk = pd.read_excel('append_Uralsk.xlsx',header=0,na_values=False)
    dataFrame_Uralsk.to_sql('Tilda_Uralsk',connector,if_exists='append',index=False)
    
connector = sqlite3.connect('data.db')
update_All()
connector.close()