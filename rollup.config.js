import { defineConfig } from 'rollup'
import dts from 'rollup-plugin-dts'

export default defineConfig({
    input: '.types/src/index.d.ts',
    output: {
        file: 'dist/index.d.ts',
        format: 'esm'
    },
    plugins: [
        dts()
    ]
})
