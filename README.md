# template maven project

自用 Maven 项目的骨架，具体依赖参考[POM](https://github.com/moqimoqidea/template-maven-project/blob/master/pom.xml)

## 使用方法

在 IntelliJ IDEA 中，提供将项目保存为模板的功能。\
以 2019.3.2 版为例，将此项目拉取到本地并使用 IntelliJ IDEA 打开，然后选择 Tools -> Save Project as Template，\
根据需要决定是否填充描述，保持最下面 Replace parameters with placeholders 前面的对勾，最后点击 OK 即可。

下次创建项目时，选择 User-defined 就能看到自己保存的项目模板，输入名称即可，IntelliJ IDEA 会完成项目名相关的替换。

## 目前未知的主要依赖

* spock 测试框架
* slf4j
* apache common lang3
* fastjson
* guava
