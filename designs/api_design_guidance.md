# API Design Rules

## 1 - Deliberately design end-to-end user workflows.

Most API developers focus on atomic methods rather than holistic workflows. They let users figure out end-to-end workflows through evolutionary happenstance, given the basic primitives they provided. The resulting user experience is often one long chain of hacks that route around technical constraints that were invisible at the level of individual methods.

To avoid this, start by listing the most common workflows that your API will be involved in. The use cases that most people will care about. Actually go through them yourself, and take notes. Better yet: watch a new user go through them, and identify pain points. Ruthlessly iron out those pain points. In particular:

- Your workflows should closely map to domain-specific notions that users care about. If you are designing an API for cooking burgers, it should probably feature unsurprising objects such as "patty", "cheese", "bun", "grill", etc. And if you are designing a deep learning API, then your core data structures and their methods should closely map to the concepts used by people familiar with the field: models/networks, layers, activations, optimizers, losses, epochs, etc.
- Ideally, no API element should deal with implementation details. You do not want the average user to deal with "primary_frame_fn", "defaultGradeLevel", "graph_hook", "shardedVariableFactory", or "hash_scope", because these are not concepts from the underlying problem domain, they are highly specific concepts that come from your internal implementation choices.
- Deliberately design the user onboarding process. How are complete newcomers going to find out the best way to solve their use case with your tool? Have an answer ready. Make sure your onboarding material closely maps to what your users care about: don't teach newcomers how your API is implemented, teach them how they can use it to solve their own problems.


## 2 - Reduce cognitive load for your users.

In the end-to-end workflows you design, always strive to reduce the mental effort that your users have to invest to understand and remember how things work. The less effort and focus you require from your users, the more they can invest in solving their actual problems -- instead of trying to figure out how to use this or that method. In particular:

- Use consistent naming and code patterns. Your API naming conventions should be internally consistent (If you usually denote counts via the num_* prefix, don't switch to n_* in some places), but also consistent with widely recognized external standards. For instance, if you are designing an API for numerical computation in Python, it should not glaringly clash with the Numpy API, which everyone uses. A user-hostile API would arbitrarily use keepdim where Numpy uses keepdims, would use dim where Numpy uses axis, etc. And an especially poorly-designed API would just randomly alternate between axis, dim, dims, axes, axis_i, dim_i, for the same concept.
- Introduce as few new concepts as possible. It's not just that additional data structures require more effort in order to learn about their methods and properties, it's that they multiply the number of mental models that are necessary to grok your API. Ideally, you should only need a single universal mental model from which everything flows (in Keras, that's the Layer/Model). Definitely avoid having more than 2-3 mental models underlying your workflows.
- Strike a balance between the number of different classes/functions you have, and the parameterization of these classes/functions. Having a different class/function for every user action induces high cognitive load, but so does parameter proliferation -- you don't want 35 keyword arguments in a class constructor. Such a balance can be achieved by making your data structures modular and composable.
- Automate what can be automated. Strive to reduce the number of user actions required in your workflows. Identify often-repeated code blocks in user-written code, and provide utilities to abstract them away. For instance, in a deep learning API, you should provide automated shape inference instead of requiring users to do mental math to compute expected input shapes in all of their layers.
- Have clear documentation, with lots of examples. The best way to communicate to the user how to solve a problem is not to talk about the solution, it is to show the solution. Make sure to have concise and readable code examples available for every feature in your API.

The litmus test I use to tell whether an API is well-designed is the following: if a new user goes through the workflow for their use case on day one (following the documentation or a tutorial), and they come back the next day to solve the same problem in a slightly different context, will they be able to follow their workflow without looking up the documentation/tutorial? Will they be able to remember their workflow in one shot? A good API is one where the cognitive load of most workflows is so low that it can be learned in one shot.

This litmus test also gives you a way to quantify how good or bad an API is, by counting the number of times the average user needs to look up information about a workflow in order to master it. The worst workflows are those that can never be fully remembered, and require following a lengthy tutorial every single time.

## 3 - Provide helpful feedback to your users.

Good design is interactive. It should be possible to use a good API while only minimally relying on documentation and tutorials -- by simply trying things that seem intuitive, and acting upon the feedback you get back from the API. In particular:

- Catch user errors early and anticipate common mistakes. Do user input validation as soon as possible. Actively keep track of common mistakes that people make, and either solve them by simplifying your API, adding targeted error messages for these mistakes, or having a "solutions to common issues" page in your docs.
- Have a place where users can ask questions. How else are you going to keep track of existing pain points you need to fix?
- Provide detailed feedback messages upon user error. A good error message should answer: what happened, in what context? What did the software expect? How can the user fix it? They should be contextual, informative, and actionable. Every error message that transparently provides the user with the solution to their problem means one less support ticket, multiplied by how many times users run into the same issue.


For example:

- In Python, the following would be an extremely bad error message:

```python
AssertionError: '1 != 3'
```
(in general, always use ValueError and avoid assert).

- Also bad:

```python
ValueError: 'Invalid target shape (600, 1).'
```

- The following is better, but still not sufficient, because it does not tell the user what they passed, and does not quite say how to fix it:

```python
ValueError: 'categorical_crossentropy requires target.shape[1] == classes'
```

- Now, here's a good example, that says what was passed, what was expected, and how to fix the issue:

```python
ValueError: '''You are passing a target array of shape (600, 1) while using as loss `categorical_crossentropy`.
`categorical_crossentropy` expects targets to be binary matrices (1s and 0s) of shape (samples, classes).
If your targets are integer classes, you can convert them to the expected format via:

--
from keras.utils import to_categorical

y_binary = to_categorical(y_int)
--

Alternatively, you can use the loss function `sparse_categorical_crossentropy` instead, which does expect integer targets.'''
```

Good error messages improve the productivity and the mood of your users.