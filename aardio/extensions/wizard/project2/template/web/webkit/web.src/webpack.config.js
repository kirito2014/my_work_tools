const { resolve } = require("path");

module.exports = {
  mode: "development",
  entry: {
    index: "./src/index.js",
  },
  output: {
    filename: "[name].js",
    path: resolve("./../web"),
  },
  resolve: {
    extensions: [".js", ".jsx", ".json", ".css"],
    alias: {
      react: "nervjs",
      "react-dom": "nervjs",
    },
  },
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        use: "babel-loader",
      },
      {
        test: /\.css$/,
        use: [
          {
            loader: 'style-loader'
          },
          {
            loader: 'css-loader' 
          },
          {
            loader: 'postcss-loader'
          }

        ],
        exclude: [resolve(__dirname, '..', 'node_modules')]
      }
    ],
  },
}; 
