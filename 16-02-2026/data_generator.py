import faker

def generate_data():
    fake = faker.Faker()
    with open('employee.csv', 'w') as f:
        f.write(f'employee_id,name,age,salary\n')
        
        for i in range(10000):
            id = i
            name = fake.name()
            age = fake.random_int(min=1, max=100)
            salary = fake.random_int(min=1000, max=100000)
            f.write(f'{id},{name},{age},{salary}\n')

if __name__ == '__main__':
    generate_data()
            
        