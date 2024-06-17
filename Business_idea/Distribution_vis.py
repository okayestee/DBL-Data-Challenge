import matplotlib.pyplot as plt

def show_vis_topics(data: dict, name: str, n_topics: int = 5, is_airline: bool = False):
    # Sorting the dictionary by values
    sorted_data = dict(sorted(data.items(), key=lambda item: item[1], reverse=True))

    # Extracting keys and values
    topics = list(sorted_data.keys())
    counts = list(sorted_data.values())

    # Calculating the total count
    total_count = sum(counts)

    # Converting counts to percentages
    percentages = [(count / total_count) * 100 for count in counts]

    topics = topics[:n_topics]
    percentages = percentages[:n_topics]

    # Creating the bar chart with percentages
    plt.figure(figsize=(10, 8))
    plt.barh(topics, percentages, color='skyblue')
    if is_airline:
        plt.xlim([0,18])
    plt.xlabel('Percentage')
    plt.ylabel('Topics')
    plt.title(f'Topics and Their Percentages for {name}')
    plt.gca().invert_yaxis()  # To display the highest values at the top
    plt.tight_layout()

    plt.savefig(f'{name}.png')
    plt.show()