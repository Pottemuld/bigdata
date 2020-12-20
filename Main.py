import time

import matplotlib.pyplot as plt, mpld3
import random

while True:
    fig = plt.figure()
    fig.subplots_adjust(top=0.8)


    ax1 = fig.add_subplot(211)
    ax1.set_ylabel('Students')
    ax1.set_xlabel('Countries')

    countries = ['DK', 'US', 'CA', 'SE', 'AU']
    students = [random.randint(1, 50), random.randint(1, 50), random.randint(1, 50), random.randint(1, 50), random.randint(1, 50)]
    print(students)
    ax1.bar(countries, students)


    html = mpld3.fig_to_html(fig=fig)
    file = open("templates/sample.html", "w")
    file.write(html)
    file.close()


    time.sleep(30)
    print("Changed")
    plt.cla()
