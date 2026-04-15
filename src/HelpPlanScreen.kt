package com.example.service

class UserService(
    val name: String,
    var age: Int
) {

    fun greet(): String {
        return "Hello, $name"
    }

    fun isAdult(): Boolean {
        if (age >= 18) {
            return true
        }
        return false
    }

    fun increaseAge(year: Int) {
        for (i in 1..year) {
            age++
        }
    }
}
