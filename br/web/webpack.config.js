const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
    entry: {
        index: './src/index.tsx',
    },
    mode: 'production',
    // mode: 'development',
    // devtool: 'inline-source-map',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
        ],
    },
    resolve: {
        extensions: ['.ts', '.tsx', '.js'],
    },
    output: {
        filename: '[name].js',
        path: path.resolve(__dirname, 'dist'),
    },
    performance: {
        // TODO: investigate how to reduce these later.
        maxEntrypointSize: 1000000,
        maxAssetSize: 1000000,
    },
    plugins: [
        new HtmlWebpackPlugin({
            title: 'TiDB Lightning',
            template: 'public/index.html',
        }),
    ],
};
