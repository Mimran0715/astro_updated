from transformers import pipeline

print("hello")
classifier = pipeline("text-classification",
                      model='bhadresh-savani/distilbert-base-uncased-emotion', 
                      return_all_scores=True)
prediction = classifier("I love using transformers. The best part is wide range of support and its easy to use", )
print(prediction)
