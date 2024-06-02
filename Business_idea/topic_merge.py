from bertopic import BERTopic

topic_model = BERTopic.load('n_gram_bertopic_model')
topic_model2 = BERTopic.load('n_gram_bertopic_model2')

merged_model = BERTopic.merge_models([topic_model, topic_model2])

merged_model.save('merged_model')
print(merged_model.get_topic_info())