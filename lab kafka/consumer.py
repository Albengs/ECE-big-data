# %%
from confluent_kafka import Consumer

path_output= "result.txt"
f= open(path_output,"w")

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'foo',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

# %%
topic='book'
consumer.subscribe([topic])

# %%
# Configuration
MAX_EMPTY_POLLS = 15  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
empty_polls = 0
error_count = 0
stop_words = [
    "a", "an", "the","\t"
    "and", "or", "but", "if", "then",
    "of", "in", "on", "at", "by", "for", "from", "to", "with",
    "as", "about", "into", "over", "after", "before", "between",
    "is", "am", "are", "was", "were", "be", "been", "being",
    "have", "has", "had",
    "do", "does", "did",
    "can", "could", "will", "would", "shall", "should",
    "may", "might", "must",
    "i", "me", "my", "mine", "myself",
    "you", "your", "yours", "yourself", "yourselves",
    "he", "him", "his", "himself",
    "she", "her", "hers", "herself",
    "it", "its", "itself",
    "we", "us", "our", "ours", "ourselves",
    "they", "them", "their", "theirs", "themselves",
    "this", "that", "these", "those",
    "who", "whom", "which", "what", "where", "when", "why", "how",
    "not", "no", "nor",
    "very", "just", "also", "too",
    "here", "there", ',','.','!','?'
]
ponct = [',','.','!','?',':',';','*',"'","/","\\",'#','(',')','[',']']

while True:
    msg = consumer.poll(1.0)
    final_line=""

    # 1. Handle "No Message" (Timeout)
    if msg is None:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            print("Closing: No new messages received.")
            break
        continue

    # 2. Handle Errors
    if msg.error():
        error_count += 1
        print(f"Consumer error: {msg.error()}")
        if error_count >= MAX_ERRORS:
            print("Closing: Too many consecutive errors.")
            break
        continue

    # 3. Handle Success
    line = msg.value().decode('utf-8').lower().split(" ")
    lineclear = ''
    for i in range(len(line)):
        if line[i] not in stop_words:
            lineclear += (" " + line[i])
    for i in range(len(lineclear)):
            if lineclear[i] not in ponct:
                final_line += lineclear[i]
    space_line = final_line.split(" ")
    final_line =''
    for i in space_line:
        if len(i):
            final_line += (" " + i) 

    

    

    # Reset counters when we actually get data
    empty_polls = 0
    error_count = 0

    print(f"treated message: {final_line}")
    f.write(final_line +"\n")


# Clean up
f.close()
consumer.close()

