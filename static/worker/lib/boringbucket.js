
function boringBucket(key, R) {
    var bucketId = 0;
    for (var i = 0; i < key.length; i++) {
        bucketId += key.charCodeAt(i);
        bucketId %= R;
    }
    return bucketId;
}

