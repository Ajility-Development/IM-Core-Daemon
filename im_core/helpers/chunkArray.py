def chunkArray(array, size):
    return [array[x:x+size] for x in range(0, len(array), size)]
