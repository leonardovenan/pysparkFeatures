from collections import namedtuple
from collections import defaultdict

Person = namedtuple('Person', ['name', 'age', 'job'])

person1 = Person('Leonard', 29, 'Data Engineer')

print(f'Name: {person1.name}\nAge: {person1.age}\nJob: {person1.job}')

word_count = defaultdict(int)

words = ['um', 'dois', 'três', 'um', 'quatro', 'um', 'cinco', 'seis', 'seis', 'três']
for word in words:
    word_count[word] += 1

print(word_count)