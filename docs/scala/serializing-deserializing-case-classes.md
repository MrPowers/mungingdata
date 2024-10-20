---
title: "Serializing and Deserializing Scala Case Classes with JSON"
date: "2020-12-23"
categories: 
  - "scala"
---

This blog post shows how to serialize and deserialize Scala case classes with the JSON file format.

Serialization is important when persisting data to disk or transferring data over the network.

The [upickle](https://github.com/lihaoyi/upickle) library makes it easy to serialize Scala case classes.

## Serializing case classes with JSON

Serializing an object means taking the data stored in an object and converting it to bytes (or a string).

Suppose you want to write the data in an object to a JSON file. JSON files store strings. JSON has no understanding about the JVM or Scala objects.

Serializing a Scala object for JSON storage means converting the object to a string and then writing it out to disk.

Start by creating a case class and instantiating an object.

```
case class City(name: String, funActivity: String, latitude: Double)
val bengaluru = City("Bengaluru", "South Indian food", 12.97)
```

Define a upickle writer and then serialize the object to be a string.

```
implicit val cityRW = upickle.default.macroRW[City]
upickle.default.write(bengaluru) // "{\"name\":\"Bengaluru\",\"funActivity\":\"South Indian food\",\"latitude\":12.97}"
```

Here's how to write the serialized object to disk.

```
os.write(
  os.pwd/"tmp"/"serialized_city.json",
  upickle.default.write(bengaluru)
)
```

Here's the content of the `serialized_city.json` file:

```
{"name":"Bengaluru","funActivity":"South Indian food","latitude":12.97}
```

[See here](https://mungingdata.com/scala/filesystem-paths-move-copy-list-delete-folders/) for more background information on how to perform Scala filesystem operations, like constructing paths and writing strings to disk.

Congratulations, you've successfully persisted the data in a Scala object to a JSON file!

## Deserializing case classes with JSON

Deserializing an object means reading data from a string / file to create a Scala object.

Lets create a string and use it to build a `City` object.

```
val str = """{"name":"Barcelona","funActivity":"Eat tapas","latitude":41.39}"""
val barcelona = upickle.default.read[City](str)
barcelona.getClass // City
barcelona.latitude // 41.39
```

upickle does all the hard work of parsing the JSON string and instantiating the City object.

Let's show how to deserialize a JSON file. Suppose you have the following `beirut.json` file.

```
{"name":"Beirut","funActivity":"Eat hummus","latitude":33.89}
```

Let's read in the JSON data and create a Scala object:

```
val path = os.pwd/"src"/"test"/"resources"/"beirut.json"
val data = os.read(path)
val beirut = upickle.default.read[City](data)
beirut.getClass // City
beirut.funActivity // "Eat hummut"
beirut.toString // "City(Beirut,Eat hummus,33.89)"
```

upickle makes it easy to convert a JSON string to a Scala object.

## Next steps

You've seen how it's easy to serialize and deserialize Scala case classes with JSON.

You can also serialize / deserialize Scala case classes with other formats, like the MessagePack binary format.

JSON is nice cause it's human readable. MessagePack is more efficient, but isn't human readable.

Make sure to understand the high level concepts around object serialization and deserialization. It's a programming design pattern that you'll encounter in a variety of domains.
