# kanshasai-prizes（ローカル開発用）

景品紹介ページ（閲覧）と、運用者向け管理画面（ログイン・追加・編集・検索・CSV取込）の最小実装です。

## 起動

1) 環境変数を用意

`.env.example` をコピーして `.env` を作成してください。

2) 依存をインストール

```bash
npm install
```

3) 開発サーバー起動

```bash
npm run dev
```

- 公開ページ: `http://localhost:3001/prizes`
- 管理画面: `http://localhost:3001/admin/login`

## データ保存について（ローカル用）

- 景品データ: `data/prizes.json`
- 管理者ユーザー: `data/users.json`
- 画像: `uploads/`

本番公開する場合は DB（Postgres/Mongo）と画像ストレージ（S3/R2等）に移行するのがおすすめです。

