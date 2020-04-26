#AP基盤プロトタイプについて

##プロジェクト構成
* databricks_dev ルートプロジェクト
  * application 業務APサブプロジェクト
  * dbconnectApplication Databricks接続（DatabricksConnect）用業務APサブプロジェクト
  * sparkFramework SparkAPのAP基盤サブプロジェクト。Spark標準機能のみに依存
  * sparkTestFramework SparkAP用の拡張テストフレームワークサブプロジェクト
  * databricksFramework Databricks固有の機能に依存するAP基盤サブプロジェクト
  * notebooks Databricks Notebookと同期されたNotebookソースコードフォルダ

##IntelliJからのAPの端末ローカルSpark実行
* 通常、APは「application」プロジェクトに作成します
* 端末ローカルでSpark実行します
* この起動方法は、Databricks依存機能は実行できません
* 別途ローカル実行用の環境セットアップが必要(Teams Wiki参照)です
  * 「構成の編集」で「アプリケーション」を作成
  * 「メインクラス」に「com.example.fw.app.ApplicationEntryPoint」を設定
  * 「プログラムの引数」に対象Logicクラスの完全修飾名を設定
    * 例）com.example.sample.logic.SampleDataSetBLogic3
  * 「VMパラメータ」に「-Dactive.profile=dev」を設定
    * または「環境変数」に「ACTIVE_PROFILE=dev」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダを設定
    * 例）C:\Users\xxxx\IdeaProjects\databricks_dev
  * 「クラスパスとJDK」は「application」を設定

##IntelliJからのAPのDatabricks接続実行
* 端末からAzure上のDatabricksクラスタに接続しSpark実行します
* Databricks依存機能も実行できます
* 別途Databrics接続実行用の環境セットアップが必要(Teams Wiki参照)です
  * 「構成の編集」で「アプリケーション」を作成
  * 「メインクラス」に「com.example.app.EntryPoint」（dbconnectApplication上のクラス）を設定
  * 「プログラムの引数」に対象Logicクラスの完全修飾名を設定
    * 例）com.example.sample.logic.SampleDataSetBLogic3
  * 「VMパラメータ」に「-Dactive.profile=dbconnect」を設定
    * または「環境変数」に「ACTIVE_PROFILE=dbconnect」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダを設定
    * 例）C:\Users\xxxx\IdeaProjects\databricks_dev
  * 「クラスパスとJDK」は「dbconnectApplication」を設定
##ビルド
* IntelliJのsbt shellの場合
    ```
    > package
    ```
* sbtコマンドの場合
  ```
  sbt package
  ```

##単体テスト
* 単体テストコードは、「application」プロジェクトの「src/test/scala」ディレクトリに格納します
* IntelliJで指定したテストクラス実行の場合
  * 「構成の編集」で「ScalaTest」を作成
  * 「テストクラス」に対象テストクラスを設定
  * 「VMパラメータ」に「-Dactive.profile=ut」を設定
    * または「環境変数」に「ACTIVE_PROFILE=ut」と設定
  * 「作業ディレクトリ」はプロジェクトのルートフォルダ
  * 「クラスパスとJDK」は「application」

* IntelliJのsbt shellの場合  
  * 「ファイル」-「設定」で、「ビルド、実行、デプロイ」の「sbt」の設定で「VMパラメータ」で「-Dactive.profile=ut」を設定しておく
  * 以下実行
  ```
  > test
  ```
* sbtコマンドの場合
  ```
  sbt -Dactive.profile=ut test
  ```
##実行可能jar（アセンブリ）作成 
* Databricks/Sparkクラスタ上でAPを実行するためには「sbt package」コマンドで生成するjarではなく、「sbt assembly」を使って必要なclassファイル等を全て1つにまとめた実行可能jarを作成します
* なお、テストをスキップするように設定済です
* 実行すると「（ルートディレクトリ）/target/scala-2.11/」フォルダに「databricks_dev-assembly-0.1.jar」が作成されます
* IntelliJのsbt shellの場合  
  ```
  > assembly
  ```
* sbtコマンドの場合
  ```
  sbt assembly
  ```
  
##Azure DevOps PipelineでのCI
* Azure Reposでソースコード管理し、Azure Pipelineでパイプラインを作成することでazure-pipelines.ymlの定義に基づきPipeline実行できます
* 現状、ビルド、単体テスト、実行可能jar作成が実行できます