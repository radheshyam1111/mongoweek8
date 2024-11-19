const mongoose = require('mongoose');

// MongoDB connection URI
const MONGO_URI = 'mongodb://localhost:27017/Week8';
mongoose.connect(MONGO_URI)
    .then(() => console.log(`Connected to ${MONGO_URI}`))
    .catch((err) => console.error("Error occurred during connection: ", err));

// Define the schema
const PersonSchema = new mongoose.Schema({
    name: { type: String, required: true },
    age: { type: Number, required: true },
    gender: { type: String, required: true },
    salary: { type: Number, required: true }
});

// Create the model
const PersonModel = mongoose.model('Person', PersonSchema, 'personCollection');

// Function to clear the collection
async function clearCollection() {
    try {
        const result = await PersonModel.deleteMany({});
        console.log(`Collection cleared. Removed ${result.deletedCount} documents.`);
    } catch (err) {
        console.error("Error clearing collection:", err);
    }
}

// Function to insert sample documents
async function insertDocuments() {
    try {
        const documents = [
            { name: 'Radhe', age: 21, gender: 'male', salary: 2000 },
            { name: 'Krishna', age: 25, gender: 'male', salary: 3000 },
            { name: 'Radhika', age: 23, gender: 'female', salary: 2500 },
            { name: 'Meera', age: 28, gender: 'female', salary: 3500 },
            { name: 'Arjun', age: 30, gender: 'male', salary: 4000 } // New record
        ];

        const count = await PersonModel.countDocuments();
        if (count === 0) {
            await PersonModel.insertMany(documents);
            console.log("New documents have been added to your database.");
        } else {
            console.log("Documents already exist in the collection.");
        }
    } catch (err) {
        console.error("Error inserting documents:", err);
    }
}

// Function to find and return valid documents
async function findDocuments() {
    try {
        const docs = await PersonModel.find({})
            .sort({ salary: 1 }) // Sort ascending by salary
            .select("name salary age") // Select only name, salary, and age fields
            .limit(10); // Limit to 10 documents

        console.log("Documents retrieved:");
        docs.forEach((doc) => {
            if (doc.name && doc.age && doc.salary) {
                console.log(`Name: ${doc.name}, Age: ${doc.age}, Salary: ${doc.salary}`);
            } else {
                console.log("Skipped document with missing fields.");
            }
        });
    } catch (err) {
        console.error("Error retrieving documents:", err);
    }
}

// Main function to handle operations
async function main() {
    await clearCollection(); // Clear the collection to remove duplicates/corrupt data
    await insertDocuments(); // Insert fresh data
    await findDocuments(); // Retrieve and log the documents
}

// Run the main function
main().catch(console.error);
