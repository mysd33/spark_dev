#AP基盤プロトタイプについて

[![Build Status](https://dev.azure.com/Masashi11Yoshida/scala_dev/_apis/build/status/databricks_dev?branchName=master)](https://dev.azure.com/Masashi11Yoshida/scala_dev/_build/latest?definitionId=1&branchName=master)

##プロジェクト構成
* databricks_dev ルートプロジェクト
  * application 業務APサブプロジェクト
  * dbconnectApplication Databricks接続（DatabricksConnect）用業務APサブプロジェクト
  * sparkFramework SparkAPのAP基盤サブプロジェクト。Spark標準機能のみに依存
  * sparkTestFramework SparkAP用の拡張テストフレームワークサブプロジェクト
  * databricksFramework Databricks固有の機能に依存するAP基盤サブプロジェクト
  * notebooks Databricks Notebookと同期されたNotebookソースコードフォルダ
  * testdata 業務APを動作させるためのテストファイル

##IntelliJからのAPの端末ローカルSpark実行
* 通常、APは「application」プロジェクトに作成します
* 端末ローカルでSpark実行します
* この起動方法は、Databricks依存機能は実行できません
* 別途ローカル実行用の環境セットアップが必要(Teams Wiki参照)です
* サンプルを動作させる際は、testdata/inputディレクトリにあるデータを「C:\temp」にコピーしてください
  * applicaitonプロジェクト/src/main/resources/application-dev.propertiesのbasepathプロパティを変更すれば違うディレクトリにも変更可能です
* AP起動方法
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

## Databricksでのライブラリ（jar）登録
* 実行可能jar「databricks_dev-assembly-0.1.jar」を下記ドキュメントの通り、ワースペースライブラリとして登録し、クラスタに同jarをインストールします
  * https://docs.microsoft.com/ja-jp/azure/databricks/libraries

## Databricksでのライブラリを用いたnotebook実行
* 上記手順でjarをクラスタにライブラリインストールすると、Notebook上で、jarに含まれるクラスを呼び出すことができます
* 以下のNotebookのscalaサンプルコードが参考になります。
  * databricks_dev\notebooks\Users\admin@mysd33.work\testFramework.scala

## Databricksでのjarジョブ実行手順
* 下記ドキュメントの通り、ジョブを作成し、jarをアップロードして実行するか、ワークスペース上のjarを指定します
  * https://docs.microsoft.com/ja-jp/azure/databricks/jobs#create-a-job

## Databricksでのnotbookジョブ実行手順
* 下記ドキュメントの通り、ジョブを作成し、notebooを指定します
  * https://docs.microsoft.com/ja-jp/azure/databricks/jobs#create-a-job
  
##Azure DevOps PipelineでのCI
* Azure Reposでソースコード管理し、Azure Pipelineでパイプラインを作成することでazure-pipelines.ymlの定義に基づきPipeline実行できます
* ビルド、scaladoc、単体テスト実行、結合テスト実行、実行可能jar作成、テスト結果レポート、カバレッジレポート、SonarQubeによる静的コード解析レポートに対応しています
* また、ivyローカルリポジトリのjarをキャッシュしビルド時間を短縮する設定もしています
* 本サンプルではAzure DevOps Serviceを使って動作確認しています
  * 本番開発では、東日本リージョン内でのDevOps Serverの構築が必要です

##Azure DevOps PipelineからのSonar Qubeの実行
* Azure DevOps Pipelineで、SonarQubeを使用した静的コードチェックを実施しています
* 利用しない場合は、azure-pipeline.ymlの該当箇所をコメントアウトしてください
  * 「task: SonarQubePrepare@4」、「task: SonarQubePrepare@4」、「task: SonarQubePublish@4」
* Azure上でのSonarQubeの簡単な構築方法は、以下の通りです
  * 本番ではそのままの手順で構築しないこと
  * （参考ページ）https://azuredevopslabs.com/labs/vstsextend/sonarqube/

* Azure Container InstancesでSonarQubeのコンテナを起動します
```
az container create -g (リソースグループ名)  --name sonarqubeaci --image sonarqube --ports 9000 --dns-name-label （DNSラベル名）--cpu 2 --memory 3.5
（例）
az container create -g RG_MYSD_DEV  --name sonarqubeaci --image sonarqube --ports 9000 --dns-name-label myd33sonarqube --cpu 2 --memory 3.5
```
* SonarQubeのコンテナのFQDNを調べて、ブラウザ起動します
  * 「http://（az containerコマンドの--dns-name-labelオプションの値）.（リージョン）.azurecontainer.io:9000」になります  
  * （例）http://myd33sonarqube.japaneast.azurecontainer.io:9000

* SonarQubeにログインしプロジェクトを作成します
  * admin/adminでログイン（ログイン後、パスワード変更しておくこと）
  * プロジェクトを作成
    * name: databricks_dev
    * projectkey: databricks_dev
    * visibility: private
  * プロジェクトのページに移動し、トークンを生成し、端末にコピーし保管します
  * nameやprojectkeyはsonar-project.propertiesの値と合わせていますので変更する場合は、sonar-project.propertiesの値も修正してください
    
* Azure PipelineのExtensionとしてSonarQubeをインストール
  * Azure DevOpsのOrganization Sttings->Exntensionsで「Browse MarketPlace」から「SonarQube」を取得
    * Azure DevOps Organizaiont管理者へのExtensionインストールの承認フローが入りインストールを完了させます

* SonarQube Serverの接続情報を登録します 
  * Project Setting -> Piplines -> Service connectionsへ移動
  * Create Service Connectionをクリック
  * 「SonarQube Server」を選択し、「Server Url」と「Token」「Service connection name」を入力します
  * 「Service connection name」はazure-pipelines.yamlの「task: SonarQubePrepare@4」の「SonarQube」の設定と合わせて'SonarQube'とします。
    *  'SonarQube'以外の名前にしたい場合は、azure-pipeline.yamlの値を直してください

## Azure DevOpsからのCD
 * 参考
   * https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/ci-cd/ci-cd-azure-devops#define-your-release-pipeline
 * 初めて使用する場合、MarketPlaceから「Databricks Script Deployment Task by Data Thirst」のExtentionを追加
   * https://marketplace.visualstudio.com/items?itemName=riserrad.azdo-databricks&targetId=97d4df0c-2f78-42f0-844c-1ba9d56e3f8a
   * Azure DevOpsのOrganization Sttings->Exntensionsで、Azure DevOps Organizaiont管理者へのExtensionインストールの承認フローが入りインストールを完了させます
 * PipelinesからReleaseパイプラインを作成   
 * 「Artifacts」で「Add」をクリックし、ビルドパイプラインで出力したアーティファクトの設定
   * Project、Source（ビルドパイプラインのリポジトリ）、Default Version、Source aliasを設定
 * 「Stages」で「Add」をクリックし、「Empty Job」（空のジョブ）を追加 
 * Add Taskでタスクを追加し「Use Python version」を追加
   * Version specは3.x   
 * Add Taskでタスクを追加し「Databricks DBFS File Deployment」を追加
   * jarをDBFS上にインストール
   * Azure Resionは、「japaneast」
   * Local Root Folderは、「$(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/target/scala-2.11/」
   * Target folder in DBFSは、「/FileStore/jars/」
   * Bearer Tokenを設定（事前に設定した環境変数から取得するようにする）
 * Add Taskでタスクを追加し「Command Line Script」を追加
   * pip install requests 
 * Add Taskでタスクを追加し「Python script」を追加   
   * cd-scripts/installLibrary.pyをつかってDatabricks上にjarインストール
   * Script Pathは「$(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/cd-scripts/installLibrary.py」
     * Databricks REST API（Library API)を使用しています。
     * REST APIの仕様上、クラスタが終了状態（Terminated）か存在しない場合にはインストールできないためパイプラインはエラーになります
     * https://docs.microsoft.com/ja-jp/azure/databricks/dev-tools/api/latest/libraries
   * Argumentsに以下を指定
     * --shard=XXX --token=XXX --clusterid=XXX --libs=XXX --dbfspath=XXX
     * shard - ワークスペースのURL
       * 例：https://xxxx.azuredatabricks.net
       * 最後にスラッシュを入れないこと
     * token - ワークスペースのアクセストークン（personal access token）
     * clusterid - クラスタID
     * libs - jarを含んでいた元のライブラリフォルダ
       * $(System.DefaultWorkingDirectory)/_databricks_dev/applicationAssembly/target/scala-2.11/
     * dbfspath - DFS上のライブラリのパス
       * /FileStore/jars
       * 最後にスラッシュを入れないこと 



