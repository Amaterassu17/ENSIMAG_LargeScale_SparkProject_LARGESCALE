import matplotlib.pyplot as plt

thresholds = []
results = []
with open('./results/question5/thresholds_results.txt', "r") as f:
    
    for line in f:
        line = line.split()
        thresholds.append(int(line[0]))
        results.append(float(line[1]))


plt.bar(thresholds, results, align='center', alpha=0.5, color='green')

plt.xlabel('Number of unique machines')
plt.ylabel('Percentage of jobs having their tasks running on same machine(s)')
plt.title('Percentage of jobs\' tasks on the same machine(s) for different number of unique machines')
plt.xticks(thresholds)

plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
plt.savefig('./results/question5/percentage_of_jobs_tasks_on_same_machines.png')

