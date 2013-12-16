wcf-reflection-ef-provider
==========================

WCF Data Services reflection provider with full Entity Framework support

==========================

WCF Data Services supports direct EF context manipulation, but what if there's an additional abstraction layer of services and repositories between Entity Framework and WCF Data Services so that context cannot be directly exposed?
Reflection provider isn't an option since it produces expression trees that are incompatible with EF and also still requres some customization, especially for service operations.
This project is a reworked Reflection Provider that uses reflection to build EDM model, but fully supports Entity Framework exposed via layers of abstraction.