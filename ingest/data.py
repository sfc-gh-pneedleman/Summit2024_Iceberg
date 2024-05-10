import random
import string
from faker import Faker
from mdgen import MarkdownPostProvider


txn_id = ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() + "%0.11d" % random.randint(1,99999999999)
print(txn_id)
print(' --- ')
fake = Faker('en_US') 
fake.add_provider(MarkdownPostProvider)

#print(fake.address())
##datetime.now().strftime("%m/%d/%Y %I:%M:%S.%f %p"),
        # // "txn_quantity": random.randint(1, 30),
        # // "customer_id":  ''.join(random.choice(customer_list)),
        # // "product_id": ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() + str(random.randint(1, 9)) +'-' + "%0.7d" % random.randint(1,9999999) + ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 1))).capitalize() ,
        # // "product_unit_price": random.randint(100, 90000) / 100,
        # // "product_desc": ''.join(random.choices(string.ascii_lowercase, k=random.randint(6, 20))).capitalize(),
        # // "payment_method": ''.join(random.choice(payment_list)),

#print(fake.paragraph(nb_sentences=2))

#print(fake.sentence())

#fake_post = fake.post(size='small') # available sizes: 'small', 'medium', 'large'
#print(fake_post)

adr= ' '.join(fake.address().splitlines())


print(adr)


from os.path import join, dirname
from faker import Faker
from dotenv import load_dotenv
from os.path import join, dirname
import os

fake = Faker('en_US')
## connect to snowflake to get a list of customer_ids
dotenv_path = join(dirname(__file__),'../.env')
print(dotenv_path)
load_dotenv(dotenv_path)

print(os.getenv("SF_ACCOUNT"))