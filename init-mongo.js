db = db.getSiblingDB('sample_mflix');

db.createUser({
  user: "samirziani",
  pwd: "samir5636123",
  roles: [{ role: "readWrite", db: "sample_mflix" }]
});

db.createCollection("articles");
