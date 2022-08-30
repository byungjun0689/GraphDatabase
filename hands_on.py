from itertools import product
from xml.dom.minicompat import NodeList
from neo4j import GraphDatabase
import pandas as pd 

uri = "neo4j://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "wjddks"))

# 데이터 Schema 
# Neo4j - RDS to GDB(Neo4j) Notion Check

file_path = './neo4j/data/'
"""
order_df = pd.read_csv(file_path + 'orders.csv')
product_df = pd.read_csv(file_path + 'products.csv')
category_df = pd.read_csv(file_path + 'categories.csv')
cusotmer_df = pd.read_csv(file_path + 'customers.csv')
employee_df = pd.read_csv(file_path + 'employees.csv')
supplier_df = pd.read_csv(file_path + 'suppliers.csv')
"""

#order_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/orders.csv")
#product_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/products.csv")
#category_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/categories.csv")
#cusotmer_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/customers.csv")
#employee_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/employees.csv")
#supplier_df = pd.read_csv("https://raw.githubusercontent.com/byungjun0689/GraphDatabase/main/neo4j/data/suppliers.csv")

# order 
def create_order(tx, orderid, shipname, customerid):
    cypher_sql = '''
        MERGE(order:Order{orderID:$orderid})
        ON CREATE 
            SET order.shipName=$shipname
            SET order.customerID=$customerid
    '''
    tx.run(cypher_sql,orderid=orderid,shipname=shipname,customerid=customerid)

def get_order_all(tx):
    result = tx.run("MATCH (o:Order) return count(o)")
    return result

def delete_all_order(tx):
    tx.run("MATCH (o:Order) DETACH DELETE o")

# product
# DataFrame column : ProductID, ProductName, UnitPrice
def create_product(tx, product_id, product_name, unit_price):
    cypher_sql = '''
        MERGE (product:Product {productID: $product_id})
            ON CREATE SET product.productName = $product_name, product.unitPrice = toFloat($unit_price);
    '''
    tx.run(cypher_sql, product_id=product_id, product_name=product_name, unit_price=unit_price)


# Supplier 
# SupplierID, CompanyName
def create_supplier(tx, supplier_id,company_name):
    cypher_sql = '''
    MERGE (supplier:Supplier {supplierID: $supplier_id})
        ON CREATE SET supplier.companyName = $company_name;
    '''
    tx.run(cypher_sql, supplier_id=supplier_id, company_name=company_name)


# Employee 
# EmployeeID, FirstName, LastName, Title
def create_employee(tx, employee_id, first_name, last_name, title):
    cypher_sql = '''
    MERGE (e:Employee {employeeID:$employee_id})
    ON CREATE SET e.firstName = $first_name, e.lastName = $last_name, e.title = $title;
    '''
    tx.run(cypher_sql, employee_id=employee_id, first_name=first_name, last_name=last_name, title=title)

# Category
# CategoryID, CategoryName, Description
def create_category(tx, category_id, category_name, description):
    cypher_sql = '''
    MERGE (c:Category {categoryID: $category_id})
        ON CREATE SET c.categoryName = $category_name, c.description = $description;
    '''
    tx.run(cypher_sql, category_id=category_id, category_name=category_name, description=description)

#customer
# CustomerID, CompanyName, ContactName
def create_customer(tx,customer_id, company_name, contact_name):
    cypher_sql='''
    MERGE (e:Customer {CustomerID:$customer_id})
    ON CREATE SET e.CompanyName = $company_name, e.ContactName = $contact_name;
    '''
    tx.run(cypher_sql,customer_id=customer_id,company_name=company_name,contact_name=contact_name)


# delete all node
def delete_all_node():
    node_list = ['Order', 'Category', 'Customer', 'Employee','Product','Supplier']
    with driver.session() as session:
        for node in node_list:
            session.write_transaction(delete_node, node)

def delete_node(tx, node):
    cyphper_sql = '''
        MATCH (n:{node})
        detach delete n
    '''.format(node=node)
    print(cyphper_sql)
    tx.run(cyphper_sql)

def create_all_node():
    with driver.session() as session:
        # create order
        for idx, row in order_df.iterrows():
            session.write_transaction(create_order, row['OrderID'], row['CustomerID'], row['ShipName'])
        
        #create product
        for idx, row in product_df.iterrows():
            session.write_transaction(create_product, row['ProductID'], row['ProductName'], row['UnitPrice'])

        #create supplier
        for idx, row in supplier_df.iterrows():
            session.write_transaction(create_supplier, row['SupplierID'], row['CompanyName']) 
        
        #create Employee
        for idx, row in employee_df.iterrows():
            session.write_transaction(create_employee, row['EmployeeID'], row['FirstName'], row['LastName'], row['Title'])
        
        #create Category
        for idx, row in category_df.iterrows():
            session.write_transaction(create_category, row['CategoryID'], row['CategoryName'], row['Description'])
        
        #creatre customer
        for idx, row in cusotmer_df.iterrows():
            session.write_transaction(create_customer, row['CustomerID'], row['CompanyName'], row['ContactName'])

def run_query(input_query):
    """
    - input_query를 전달받아서 실행하고 그 결과를 출력하는 함수입니다.
    """
    # 세션을 열어줍니다.
    results_list = []
    with driver.session() as session: 
        # 쿼리를 실행하고 그 결과를 results에 넣어줍니다.
        results = session.run(
            input_query
        )
        results_list = list(results)
    return results_list

def create_index():
    index_list = ["CREATE INDEX product_id FOR (p:Product) ON (p.productID)",
    "CREATE INDEX product_name FOR (p:Product) ON (p.productName)",
    "CREATE INDEX supplier_id FOR (s:Supplier) ON (s.supplierID)",
    "CREATE INDEX employee_id FOR (e:Employee) ON (e.employeeID)",
    "CREATE INDEX category_id FOR (c:Category) ON (c.categoryID)",
    "CREATE CONSTRAINT order_id ON (o:Order) ASSERT o.orderID IS UNIQUE"
    "CALL db.awaitIndexes()"]
    for index_sql in index_list:
        result = run_query(index_sql)
        print(result)

## Relation 생성 ##
# Order와 Product 관계 생성
def create_relation_Order_Product(tx, row):
    cypher_sql = """
    MATCH (order:Order {orderID: $OrderID})
    MATCH (product:Product {productID: $ProductID})
    MERGE (order)-[op:CONTAINS]->(product)
        ON CREATE SET op.unitPrice = toFloat($UnitPrice), op.quantity = toFloat($Quantity);
    """
    tx.run(cypher_sql, OrderID=row["OrderID"], ProductID=row["ProductID"], UnitPrice=row["UnitPrice"], Quantity=row["Quantity"])

# Order와 Employee 관계 생성
def create_relation_Order_Employee(tx,row):
    cypher_sql="""
    MATCH (order:Order {orderID: $OrderID})
    MATCH (employee:Employee {employeeID: $EmployeeID})
    MERGE (employee)-[:SOLD]->(order);
    """
    tx.run(cypher_sql, OrderID=row["OrderID"], EmployeeID=row["EmployeeID"])

# Customer, Order 관계 생성(Purchase)
def create_relation_Order_Customer(tx, row):
    cypher_sql = """
    MATCH (order:Order {orderID: $OrderID})
    MATCH (customer:Customer {CustomerID: $CustomerID})
    MERGE (customer)-[:PURCHASE]->(order);
    """
    tx.run(cypher_sql, OrderID=row["OrderID"], CustomerID=row["CustomerID"])

# Products and Suppliers 관계 생성
def create_relation_Product_Suppliers(tx,row):
    cypher_sql="""
    MATCH (product:Product {productID: $ProductID})
    MATCH (supplier:Supplier {supplierID: $SupplierID})
    MERGE (supplier)-[:SUPPLIES]->(product);
    """
    tx.run(cypher_sql, ProductID=row["ProductID"], SupplierID=row["SupplierID"])

# Products and Categories 관계 생성
def create_relation_Product_Category(tx, row):
    cypher_sql="""
    MATCH (product:Product {productID: $ProductID})
    MATCH (category:Category {categoryID: $CategoryID})
    MERGE (product)-[:PART_OF]->(category);
    """
    tx.run(cypher_sql, ProductID=row["ProductID"], CategoryID=row["CategoryID"])

# Employees 간의 관계 설정(reportTo)
def create_relation_Employee_Each(tx, row):
    cypher_sql = """
    MATCH (employee:Employee {employeeID: $EmployeeID})
    MATCH (manager:Employee {employeeID: $ReportsTo})
    MERGE (employee)-[:REPORTS_TO]->(manager);
    """
    tx.run(cypher_sql, EmployeeID=row["EmployeeID"], ReportsTo=row["ReportsTo"])

def create_all_relation():
    with driver.session() as session:
        
        for idx, row in order_df.iterrows():
            session.write_transaction(create_relation_Order_Product, row)
            session.write_transaction(create_relation_Order_Employee, row)
            session.write_transaction(create_relation_Order_Customer, row)
        
        for idx, row in product_df.iterrows():
            session.write_transaction(create_relation_Product_Suppliers, row)
            session.write_transaction(create_relation_Product_Category, row)
        
        for idx, row in employee_df.iterrows():
            session.write_transaction(create_relation_Employee_Each, row)

#delete_all_node()
#create_all_node()
#create_index()
#create_all_relation()
# 쿼리 샘플

# 제품 단가가 높은 순대로 정렬.
cypher_sql = """
    MATCH (p:Product)
RETURN p.productName, p.unitPrice
ORDER BY p.unitPrice DESC
LIMIT 10;
"""

print("Product list order by unit price ")
results = run_query(cypher_sql)
for product in results:
    tmp = {
        "product_name" : product["p.productName"],
        "unit_price": product["p.unitPrice"]
    }
    print(tmp)

print("="*100)

# 초콜렛을 주문한 회사 이름을 알고 싶다
cypher_sql = """
MATCH (p:Product {productName:"Chocolade"})<-[:CONTAINS]-(:Order)<-[:PURCHASE]-(c:Customer)
RETURN distinct c.CompanyName;
"""

print("초콜렛을 주문한 회사 이름을 알고 싶다")
results = run_query(cypher_sql)
for row in results:
    tmp = {
        "company_name" : row["c.CompanyName"]
    }
    print(tmp)
print("=" * 100)

# 특정회사에서 지금까지 구매 했던 제품 목록과 총 금액
print("Drachenblut Delikatessen에서 지금까지 구매 했던 제품 목록과 총 금액")

cypher_sql = """
MATCH (c:Customer{CompanyName:"Drachenblut Delikatessen"})
OPTIONAL MATCH(p:Product)<-[pu:CONTAINS]-(:Order)<-[:PURCHASE]-(c)
return p.productName as productName, toInteger(sum(pu.unitPrice * pu.quantity)) as volume
order by volume desc
"""

results = run_query(cypher_sql)
for row in results:
    tmp = {
        "productName" : row["productName"],
        "volume" : row["volume"]
    }
    print(tmp)
print("=" * 100)

# 특정 제품을 공급하는 업체는?
def read_product(tx):
    return list(tx.run("MATCH (s:Supplier)-[r1:SUPPLIES]->(p:Product {productName: 'Konbu'})-[r2:PART_OF]->(c:Category) RETURN s, r1, p, r2, c;"))

with driver.session() as session:
    supplies = session.read_transaction(read_product)

    for row in supplies:
        tmp = {
            "product_name":row["p"]["productName"],
            "supply_company_name":row["s"]["companyName"],
            "category" : row["c"]["categoryName"]
        }
        print(tmp)
    #for s in supplies:
    #    print(s)

# 초콜릿과 Cross-Selling 한 직원과 어떤제품을 많이 팔았는가.
def read_cross_selling(tx, product_name):
    cypher_sql = """
    MATCH (choc:Product {productName:$product_name})<-[:CONTAINS]-(:Order)<-[:SOLD]-(employee),
      (employee)-[:SOLD]->(o2)-[:CONTAINS]->(other:Product)
    RETURN employee.employeeID as employee, other.productName as otherProduct, count(distinct o2) as count
    ORDER BY count DESC
    LIMIT 5;
    """
    return list(tx.run(
        cypher_sql, 
        product_name=product_name
    ))

with driver.session() as session:
    product_list = session.read_transaction(read_cross_selling, 'Chocolade')
    for product in product_list:
        tmp = {
            "employee_id":product['employee'],
            "other_product":product["otherProduct"],
            "sell_count":product["count"]
        }
        print(tmp)

# 어떤직원이 어떤 직원에게 보고하고 있는가?